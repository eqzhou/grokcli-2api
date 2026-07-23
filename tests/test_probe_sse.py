from __future__ import annotations


def test_responses_probe_requires_text_and_completed() -> None:
    from grok2api.pool.probe_sse import parse_probe_sse

    result = parse_probe_sse(
        [
            'data: {"type":"response.output_text.delta","delta":"OK"}',
            'data: {"type":"response.completed","response":{"status":"completed"}}',
        ]
    )

    assert result["outcome"] == "success"
    assert result["probe_status"] == "ok"
    assert result["ok"] is True
    assert result["available"] is True
    assert result["text"] == "OK"


def test_responses_completed_output_text_is_strong_success() -> None:
    from grok2api.pool.probe_sse import parse_probe_sse

    result = parse_probe_sse(
        [
            'data: {"type":"response.completed","response":{"status":"completed",'
            '"output":[{"content":[{"type":"output_text","text":"OK"}]}]}}'
        ]
    )

    assert result["outcome"] == "success"
    assert result["text"] == "OK"


def test_completed_without_text_is_inconclusive() -> None:
    from grok2api.pool.probe_sse import parse_probe_sse

    result = parse_probe_sse(
        ['data: {"type":"response.completed","response":{"status":"completed"}}']
    )

    assert result["outcome"] == "inconclusive"
    assert result["ok"] is False
    assert result["available"] is False


def test_completed_without_response_status_is_inconclusive() -> None:
    from grok2api.pool.probe_sse import parse_probe_sse

    result = parse_probe_sse(
        [
            'data: {"type":"response.output_text.delta","delta":"OK"}',
            'data: {"type":"response.completed"}',
        ]
    )

    assert result["outcome"] == "inconclusive"
    assert result["probe_status"] == "inconclusive"


def test_done_only_is_inconclusive() -> None:
    from grok2api.pool.probe_sse import parse_probe_sse

    result = parse_probe_sse(["data: [DONE]"])

    assert result["outcome"] == "inconclusive"
    assert result["ok"] is False


def test_responses_error_event_is_failure_even_after_text() -> None:
    from grok2api.pool.probe_sse import parse_probe_sse

    result = parse_probe_sse(
        [
            'data: {"type":"response.output_text.delta","delta":"OK"}',
            'data: {"type":"response.failed","response":{"error":{"message":"capacity"}}}',
        ]
    )

    assert result["outcome"] == "failure"
    assert result["probe_status"] == "fail"
    assert result["ok"] is False
    assert "capacity" in result["error"]


def test_responses_incomplete_event_is_failure() -> None:
    from grok2api.pool.probe_sse import parse_probe_sse

    result = parse_probe_sse(
        [
            'data: {"type":"response.incomplete","response":'
            '{"incomplete_details":{"reason":"max_output_tokens"}}}'
        ]
    )

    assert result["outcome"] == "failure"
    assert "max_output_tokens" in result["error"]


def test_chat_probe_requires_text_finish_reason_and_done() -> None:
    from grok2api.pool.probe_sse import parse_probe_sse

    result = parse_probe_sse(
        [
            'data: {"choices":[{"delta":{"content":"OK"},"finish_reason":null}]}',
            'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}',
            "data: [DONE]",
        ]
    )

    assert result["outcome"] == "success"
    assert result["protocol"] == "chat_completions"


def test_chat_text_without_normal_end_is_inconclusive() -> None:
    from grok2api.pool.probe_sse import parse_probe_sse

    result = parse_probe_sse(
        ['data: {"choices":[{"delta":{"content":"OK"},"finish_reason":null}]}']
    )

    assert result["outcome"] == "inconclusive"


def test_malformed_event_cannot_be_washed_into_success() -> None:
    from grok2api.pool.probe_sse import parse_probe_sse

    result = parse_probe_sse(
        [
            "data: {bad json}",
            'data: {"type":"response.output_text.delta","delta":"OK"}',
            'data: {"type":"response.completed","response":{"status":"completed"}}',
        ]
    )

    assert result["outcome"] == "inconclusive"


def test_oversized_sse_line_is_inconclusive() -> None:
    from grok2api.pool.probe_sse import parse_probe_sse

    result = parse_probe_sse(["data: " + ("x" * (1024 * 1024 + 1))])

    assert result["outcome"] == "inconclusive"
    assert result["probe_status"] == "inconclusive"
    assert "size limit" in result["error"]


def test_event_only_failure_is_failure() -> None:
    from grok2api.pool.probe_sse import parse_probe_sse

    result = parse_probe_sse(["event: response.failed", ""])

    assert result["outcome"] == "failure"


def test_multiline_data_is_assembled_as_one_event() -> None:
    from grok2api.pool.probe_sse import parse_probe_sse

    result = parse_probe_sse(
        [
            "event: response.failed",
            'data: {"type":',
            'data: "response.failed", "error":{"message":"blocked"}}',
            "",
        ]
    )

    assert result["outcome"] == "failure"
    assert "blocked" in result["error"]


def test_raw_chunks_are_bounded_before_line_decoding() -> None:
    from grok2api.pool.probe_sse import parse_probe_sse

    result = parse_probe_sse(
        [b"data: " + (b"x" * (1024 * 1024 + 1))], raw_chunks=True
    )

    assert result["outcome"] == "inconclusive"
    assert "size limit" in result["error"]
