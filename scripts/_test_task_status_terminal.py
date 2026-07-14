#!/usr/bin/env python3
"""Regression: Claude Code → sub2api → grokcli-2api task status / terminal frames.

Root causes previously observed:
1. Bare error without message_delta+message_stop → agent hangs "running"
2. Incomplete Update/Edit shipped as "{}" → tool fails, status freezes
3. new_string="" treated incomplete → Update never ships / hangs
4. Responses complete() setting _closed on empty → fail() no-op, missing DONE
"""
from __future__ import annotations

import json
import sys


def _parse_sse(frames: list[str]) -> list[dict]:
    payloads: list[dict] = []
    for e in frames:
        for line in e.splitlines():
            if line.startswith("data:"):
                raw = line[5:].strip()
                if not raw or raw == "[DONE]":
                    continue
                try:
                    payloads.append(json.loads(raw))
                except Exception:
                    pass
    return payloads


def main() -> int:
    import anthropic_compat as a
    import openai_responses as o

    print("=== tool readiness ===")
    cases = [
        ("Update", '{"file_path":"/x","old_string":"a","new_string":""}', True),
        ("Edit", '{"file_path":"/x","old_string":"a","new_string":""}', True),
        ("Update", '{"file_path":"/x","old_string":"a"}', False),
        ("Update", '{"file_path":"/x"}', False),
        ("Read", '{"file_path":""}', False),
        ("Read", '{"file_path":"/x"}', True),
        ("Bash", '{"command":"ls"}', True),
        ("Write", '{"file_path":"/x","content":""}', True),
        # Critical: Task*/Todo* must NOT inherit Update/Write required keys.
        ("TaskUpdate", '{"taskId":"1","status":"completed"}', True),
        ("TaskCreate", '{"subject":"x","description":"y"}', True),
        ("TodoWrite", '{"todos":[{"content":"a","status":"pending","activeForm":"doing"}]}', True),
        ("mcp__x__Update", '{"file_path":"/x","old_string":"a","new_string":"b"}', True),
        ("mcp__x__Update", '{"file_path":"/x"}', False),
        ("company_Update", '{"file_path":"/x","old_string":"a","new_string":"b"}', True),
    ]
    for name, args, expect in cases:
        got = a.is_complete_tool_arguments_json(args, tool_name=name)
        print(f"  {name:16} complete={got} expect={expect} raw={args[:70]}")
        assert got is expect, f"{name} readiness mismatch: {got} != {expect}"

    print("\n=== required-key suffix must not swallow TaskUpdate/TodoWrite ===")
    assert a._required_keys_for_tool("TaskUpdate") == ()
    assert a._required_keys_for_tool("TaskCreate") == ()
    assert a._required_keys_for_tool("TodoWrite") == ()
    assert a._required_keys_for_tool("Update") == (
        "file_path",
        "old_string",
        "new_string",
    )
    assert a._required_keys_for_tool("Write") == ("file_path", "content")
    assert a._required_keys_for_tool("mcp__x__Update") == (
        "file_path",
        "old_string",
        "new_string",
    )
    assert a._required_keys_for_tool("company_Update") == (
        "file_path",
        "old_string",
        "new_string",
    )
    print("  suffix boundary OK")

    print("\n=== terminal_error envelope ===")
    evs = a.anthropic_stream_terminal_error("boom")
    types = []
    for e in evs:
        et = None
        data = None
        for line in e.splitlines():
            if line.startswith("event:"):
                et = line.split(":", 1)[1].strip()
            if line.startswith("data:"):
                data = json.loads(line[5:].strip())
        stop = None
        if isinstance(data, dict):
            stop = (data.get("delta") or {}).get("stop_reason")
        types.append((et, data.get("type") if isinstance(data, dict) else None, stop))
    print("  types:", types)
    assert any(t[0] == "error" for t in types)
    assert any(t[0] == "message_delta" for t in types)
    assert any(t[0] == "message_stop" for t in types)
    assert any(t[2] == "end_turn" for t in types)
    print("  terminal_error OK")

    print("\n=== assembler: incomplete Update must NOT open / invent {} ===")
    asm = a.AnthropicStreamAssembler(
        message_id="msg_test1",
        model="grok-4.5",
        tools_requested=True,
        max_tools=1,
    )
    frames = asm.feed(
        tool_calls=[
            {
                "index": 0,
                "id": "toolu_test1",
                "function": {
                    "name": "Update",
                    "arguments": '{"file_path":"/x"}',
                },
            }
        ]
    )
    fin = asm.finish(finish_reason="tool_calls")
    payloads = _parse_sse(frames + fin)
    kinds = [p.get("type") for p in payloads]
    starts = [p for p in payloads if p.get("type") == "content_block_start"]
    print("  finish kinds:", kinds)
    print("  starts:", starts)
    assert any(p.get("type") == "message_stop" for p in payloads)
    assert any(p.get("type") == "message_delta" for p in payloads)
    assert not any(
        (p.get("content_block") or {}).get("name") == "Update" for p in starts
    ), "must not open incomplete Update"
    # No invented empty tool input
    deltas = [
        p
        for p in payloads
        if p.get("type") == "content_block_delta"
        and (p.get("delta") or {}).get("type") == "input_json_delta"
    ]
    assert not any((p.get("delta") or {}).get("partial_json") == "{}" for p in deltas)
    print("  incomplete Update OK")

    print("\n=== assembler: Update with new_string='' must ship ===")
    asm2 = a.AnthropicStreamAssembler(
        message_id="msg_test2",
        model="grok-4.5",
        tools_requested=True,
        max_tools=1,
    )
    args = '{"file_path":"/x","old_string":"a","new_string":""}'
    frames2 = asm2.feed(
        tool_calls=[
            {
                "index": 0,
                "id": "toolu_test2",
                "function": {"name": "Update", "arguments": args},
            }
        ]
    )
    fin2 = asm2.finish(finish_reason="tool_calls")
    payloads2 = _parse_sse(frames2 + fin2)
    starts2 = [p for p in payloads2 if p.get("type") == "content_block_start"]
    names2 = [(p.get("content_block") or {}).get("name") for p in starts2]
    print("  starts:", names2)
    assert "Update" in names2
    assert any(p.get("type") == "message_stop" for p in payloads2)
    assert any(p.get("type") == "content_block_stop" for p in payloads2)
    # stop_reason should be tool_use
    deltas_msg = [p for p in payloads2 if p.get("type") == "message_delta"]
    assert deltas_msg, "need message_delta"
    stop = (deltas_msg[-1].get("delta") or {}).get("stop_reason")
    print("  stop_reason:", stop)
    assert stop == "tool_use"
    print("  empty new_string Update OK")

    print("\n=== assembler: TaskUpdate complete ships + terminal ===")
    asm3 = a.AnthropicStreamAssembler(
        message_id="msg_test3",
        model="grok-4.5",
        tools_requested=True,
        max_tools=1,
    )
    targs = '{"taskId":"1","status":"completed"}'
    frames3 = asm3.feed(
        tool_calls=[
            {
                "index": 0,
                "id": "toolu_task",
                "function": {"name": "TaskUpdate", "arguments": targs},
            }
        ]
    )
    fin3 = asm3.finish(finish_reason="tool_calls")
    payloads3 = _parse_sse(frames3 + fin3)
    starts3 = [
        (p.get("content_block") or {}).get("name")
        for p in payloads3
        if p.get("type") == "content_block_start"
    ]
    print("  starts:", starts3)
    assert "TaskUpdate" in starts3
    assert any(p.get("type") == "message_stop" for p in payloads3)
    print("  TaskUpdate terminal OK")

    print("\n=== ResponsesLiveStreamer: empty complete leaves fail() usable ===")
    s = o.ResponsesLiveStreamer(response_id="resp_test", model="grok-4.5")
    s.start()
    empty = s.complete(usage={"prompt_tokens": 1, "completion_tokens": 0})
    print("  empty complete frames:", len(empty), "closed:", s._closed)
    assert empty == []
    assert s._closed is False
    fail = s.fail("empty upstream")
    print("  fail frames:", len(fail))
    assert any("response.failed" in f for f in fail)
    assert any("[DONE]" in f for f in fail)
    assert s._closed is True
    print("  empty complete → fail OK")

    print("\n=== ResponsesLiveStreamer: Update new_string='' ships completed ===")
    s2 = o.ResponsesLiveStreamer(response_id="resp_test2", model="grok-4.5")
    s2.start()
    s2.on_tool_delta(
        [
            {
                "index": 0,
                "id": "call_1",
                "function": {
                    "name": "Update",
                    "arguments": '{"file_path":"/x","old_string":"a","new_string":""}',
                },
            }
        ]
    )
    done = s2.complete(usage={"prompt_tokens": 10, "completion_tokens": 5})
    print("  done frames:", len(done), "closed:", s2._closed)
    assert s2._closed
    assert any("response.completed" in f for f in done)
    assert any("[DONE]" in f for f in done)
    print("  Responses Update empty new_string OK")

    print("\n=== ResponsesLiveStreamer: TaskUpdate ships completed (sub2api path) ===")
    s2b = o.ResponsesLiveStreamer(response_id="resp_test2b", model="grok-4.5")
    s2b.start()
    mid = s2b.on_tool_delta(
        [
            {
                "index": 0,
                "id": "call_task",
                "function": {
                    "name": "TaskUpdate",
                    "arguments": '{"taskId":"1","status":"completed"}',
                },
            }
        ]
    )
    done2b = s2b.complete(usage={"prompt_tokens": 10, "completion_tokens": 5})
    all2b = mid + done2b
    print("  TaskUpdate frames:", len(all2b), "closed:", s2b._closed)
    assert s2b._closed
    assert any("response.completed" in f for f in all2b)
    assert any("[DONE]" in f for f in all2b)
    # function_call item should appear (not held forever)
    assert any("function_call" in f for f in all2b)
    print("  Responses TaskUpdate OK")

    print("\n=== ResponsesLiveStreamer: incomplete Update does not complete empty ===")
    s3 = o.ResponsesLiveStreamer(response_id="resp_test3", model="grok-4.5")
    s3.start()
    s3.on_tool_delta(
        [
            {
                "index": 0,
                "id": "call_2",
                "function": {
                    "name": "Update",
                    "arguments": '{"file_path":"/x"}',
                },
            }
        ]
    )
    empty3 = s3.complete(
        usage={"prompt_tokens": 1, "completion_tokens": 0},
        force_flush_partial_tools=False,
    )
    print("  incomplete complete frames:", len(empty3), "closed:", s3._closed)
    # Should not emit empty completed; leave reopenable for fail
    assert empty3 == [] or not any("response.completed" in f for f in empty3)
    if empty3 == []:
        assert s3._closed is False
        fail3 = s3.fail("incomplete tool")
        assert any("response.failed" in f for f in fail3)
        assert any("[DONE]" in f for f in fail3)
    print("  incomplete Update empty-path OK")

    print("\nALL PASS")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as e:
        print("ASSERT FAIL:", e, file=sys.stderr)
        raise
    except Exception as e:
        print("ERROR:", type(e).__name__, e, file=sys.stderr)
        raise
