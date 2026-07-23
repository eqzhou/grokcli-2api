"""Strict, side-effect-free parser for model-health probe streams."""

from __future__ import annotations

import json
from typing import Any, Iterable


_FAILURE_TYPES = {"error", "response.error", "response.failed", "response.incomplete"}
_NORMAL_CHAT_FINISH_REASONS = {"stop"}
_MAX_SSE_LINE_BYTES = 1024 * 1024
_MAX_SSE_TOTAL_BYTES = 4 * 1024 * 1024


def _error_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("message", "detail", "reason", "code", "type"):
            text = _error_text(value.get(key))
            if text:
                return text
        for nested in ("error", "incomplete_details", "response"):
            text = _error_text(value.get(nested))
            if text:
                return text
    if isinstance(value, list):
        return "; ".join(filter(None, (_error_text(item) for item in value)))
    return ""


def _response_output_text(payload: dict[str, Any]) -> str:
    response = payload.get("response")
    if not isinstance(response, dict):
        return ""
    parts: list[str] = []
    for item in response.get("output") or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content") or []:
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text)
    return "".join(parts)


def _bounded_lines(
    chunks: Iterable[str | bytes], *, raw_chunks: bool
) -> Iterable[tuple[str, str | None]]:
    """Yield bounded decoded lines and an optional terminal parse issue."""
    total_bytes = 0
    pending = bytearray()
    for raw in chunks:
        chunk = raw if isinstance(raw, bytes) else str(raw).encode("utf-8")
        total_bytes += len(chunk)
        if total_bytes > _MAX_SSE_TOTAL_BYTES:
            yield "", "probe SSE stream exceeded size limit"
            return
        if not raw_chunks:
            if len(chunk) > _MAX_SSE_LINE_BYTES:
                yield "", "probe SSE line exceeded size limit"
                return
            yield chunk.decode("utf-8", errors="replace").rstrip("\r\n"), None
            continue
        pending.extend(chunk)
        while True:
            newline = pending.find(b"\n")
            if newline < 0:
                break
            if newline > _MAX_SSE_LINE_BYTES:
                yield "", "probe SSE line exceeded size limit"
                return
            line = bytes(pending[:newline]).rstrip(b"\r")
            del pending[: newline + 1]
            yield line.decode("utf-8", errors="replace"), None
        if len(pending) > _MAX_SSE_LINE_BYTES:
            yield "", "probe SSE line exceeded size limit"
            return
    if pending:
        yield bytes(pending).decode("utf-8", errors="replace").rstrip("\r"), None


def _sse_events(
    chunks: Iterable[str | bytes], *, raw_chunks: bool
) -> Iterable[tuple[str, str, str | None]]:
    event_name = ""
    data_lines: list[str] = []

    def framed() -> tuple[str, str, str | None]:
        return event_name, "\n".join(data_lines).strip(), None

    for line, issue in _bounded_lines(chunks, raw_chunks=raw_chunks):
        if issue:
            yield "", "", issue
            return
        if line == "":
            if event_name or data_lines:
                yield framed()
            event_name = ""
            data_lines = []
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            if event_name or data_lines:
                yield framed()
                data_lines = []
            event_name = line[6:].strip()
            continue
        if not line.startswith("data:"):
            continue
        # Compatibility with simple test/fallback iterators that omit blank SSE
        # separators: a complete pending JSON value is its own event.
        if data_lines:
            pending_data = "\n".join(data_lines).strip()
            if pending_data == "[DONE]":
                yield framed()
                event_name = ""
                data_lines = []
            else:
                try:
                    json.loads(pending_data)
                except (TypeError, ValueError):
                    pass
                else:
                    yield framed()
                    event_name = ""
                    data_lines = []
        value = line[5:]
        if value.startswith(" "):
            value = value[1:]
        data_lines.append(value)
    if event_name or data_lines:
        yield framed()


def parse_probe_sse(
    chunks: Iterable[str | bytes], *, raw_chunks: bool = False
) -> dict[str, Any]:
    """Classify a bounded Chat Completions or Responses SSE stream."""
    text_parts: list[str] = []
    protocol: str | None = None
    response_completed = False
    chat_finished = False
    done = False
    failure = ""
    parse_issue = ""
    saw_data = False

    for event_hint, data, frame_issue in _sse_events(chunks, raw_chunks=raw_chunks):
        if frame_issue:
            parse_issue = frame_issue
            break
        event_type = event_hint.strip().lower()
        if not data:
            if event_type in _FAILURE_TYPES:
                failure = event_type
            continue
        saw_data = True
        if data == "[DONE]":
            done = True
            continue
        try:
            payload = json.loads(data)
        except (TypeError, ValueError):
            parse_issue = "probe SSE contained malformed JSON"
            continue
        if not isinstance(payload, dict):
            parse_issue = "probe SSE contained a non-object event"
            continue

        event_type = str(payload.get("type") or event_type).strip().lower()
        if event_type.startswith("response."):
            protocol = "responses"
        if event_type in _FAILURE_TYPES or (
            isinstance(payload.get("error"), (dict, str)) and payload.get("error")
        ):
            failure = (
                _error_text(payload.get("error") or payload.get("response") or payload)
                or event_type
                or "upstream error"
            )
            continue
        if event_type in {"response.output_text.delta", "response.output_text.done"}:
            delta = payload.get("delta") if event_type.endswith(".delta") else payload.get("text")
            if isinstance(delta, str):
                text_parts.append(delta)
        if event_type == "response.completed":
            response = payload.get("response")
            if not isinstance(response, dict) or not response.get("status"):
                parse_issue = "response.completed omitted a valid response status"
                continue
            status = str(response.get("status")).lower()
            if status != "completed":
                failure = _error_text(response) or f"response ended with status {status}"
            else:
                response_completed = True
                if not "".join(text_parts).strip():
                    completed_text = _response_output_text(payload)
                    if completed_text:
                        text_parts.append(completed_text)

        choices = payload.get("choices")
        if isinstance(choices, list):
            protocol = protocol or "chat_completions"
            for choice in choices:
                if not isinstance(choice, dict):
                    continue
                delta = choice.get("delta")
                message = choice.get("message")
                content = delta.get("content") if isinstance(delta, dict) else None
                if content is None and isinstance(message, dict):
                    content = message.get("content")
                if isinstance(content, str):
                    text_parts.append(content)
                finish_reason = choice.get("finish_reason")
                if finish_reason:
                    finish = str(finish_reason).lower()
                    if finish in _NORMAL_CHAT_FINISH_REASONS:
                        chat_finished = True
                    else:
                        failure = f"chat ended with finish_reason={finish}"

    text = "".join(text_parts).strip()
    if failure:
        outcome = "failure"
    elif parse_issue:
        outcome = "inconclusive"
    elif protocol == "responses" and text and response_completed:
        outcome = "success"
    elif protocol == "chat_completions" and text and chat_finished and done:
        outcome = "success"
    else:
        outcome = "inconclusive"

    error = failure or None
    if outcome == "inconclusive":
        if parse_issue:
            error = parse_issue
        elif not saw_data:
            error = "empty probe stream"
        elif not text:
            error = "probe stream contained no assistant text"
        else:
            error = "probe stream ended without a normal completion"
    success = outcome == "success"
    return {
        "outcome": outcome,
        "probe_status": "ok" if success else "fail" if outcome == "failure" else outcome,
        "ok": success,
        "available": success,
        "text": text[:800],
        "completed": bool(response_completed or (chat_finished and done)),
        "stream_ok": saw_data,
        "protocol": protocol,
        "error": error[:800] if isinstance(error, str) else error,
    }
