package modelhealth

import (
	"strings"
	"testing"
)

func TestClassifyResponsesProbeRequiresTextAndCompleted(t *testing.T) {
	stream := strings.Join([]string{
		`event: response.output_text.delta`,
		`data: {"type":"response.output_text.delta","delta":"OK"}`,
		``,
		`event: response.completed`,
		`data: {"type":"response.completed","response":{"status":"completed"}}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n")

	result := classifyProbeStream(strings.NewReader(stream))
	if result.Outcome != probeSuccess || result.Text != "OK" || !result.Completed {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestClassifyResponsesProbeAcceptsCompletedOutputText(t *testing.T) {
	stream := strings.Join([]string{
		`event: response.completed`,
		`data: {"type":"response.completed","response":{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"OK"}]}]}}`,
		``,
	}, "\n")

	result := classifyProbeStream(strings.NewReader(stream))
	if result.Outcome != probeSuccess || result.Text != "OK" || !result.Completed {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestClassifyResponsesProbeDoesNotDuplicateDeltaAndCompletedSnapshot(t *testing.T) {
	stream := strings.Join([]string{
		`event: response.output_text.delta`,
		`data: {"type":"response.output_text.delta","delta":"OK"}`,
		``,
		`event: response.completed`,
		`data: {"type":"response.completed","response":{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"OK"}]}]}}`,
		``,
	}, "\n")

	result := classifyProbeStream(strings.NewReader(stream))
	if result.Outcome != probeSuccess || result.Text != "OK" || !result.Completed {
		t.Fatalf("completed snapshot duplicated streamed delta: %#v", result)
	}
}

func TestClassifyResponsesProbeTerminalErrorsAreFailure(t *testing.T) {
	for _, eventType := range []string{
		"response.failed",
		"response.error",
		"response.incomplete",
		"error",
	} {
		t.Run(eventType, func(t *testing.T) {
			stream := "event: " + eventType + "\n" +
				`data: {"type":"` + eventType + `","error":{"code":"bad","message":"nope"}}` + "\n\n"
			result := classifyProbeStream(strings.NewReader(stream))
			if result.Outcome != probeFailure || result.ErrorCode != "bad" {
				t.Fatalf("unexpected result: %#v", result)
			}
		})
	}
}

func TestClassifyResponsesProbeInconclusiveCases(t *testing.T) {
	tests := map[string]string{
		"empty":     "",
		"only done": "data: [DONE]\n\n",
		"completed empty": `event: response.completed
data: {"type":"response.completed","response":{"status":"completed","output":[]}}

`,
		"partial text": `event: response.output_text.delta
data: {"type":"response.output_text.delta","delta":"OK"}

`,
		"malformed completed": `event: response.output_text.delta
data: {"type":"response.output_text.delta","delta":"OK"}

event: response.completed
data: {"type":"response.completed"}

`,
		"malformed": "data: {bad json}\n\n",
	}
	for name, stream := range tests {
		t.Run(name, func(t *testing.T) {
			result := classifyProbeStream(strings.NewReader(stream))
			if result.Outcome != probeInconclusive {
				t.Fatalf("unexpected result: %#v", result)
			}
		})
	}
}

func TestClassifyChatProbeRequiresTextAndNormalFinish(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"OK"}}]}`,
		``,
		`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n")
	result := classifyProbeStream(strings.NewReader(stream))
	if result.Outcome != probeSuccess || result.Text != "OK" || !result.Completed {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestClassifyChatProbeDoesNotDuplicateDeltaAndFinalMessage(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"OK"}}]}`,
		``,
		`data: {"choices":[{"message":{"content":"OK"},"finish_reason":"stop"}]}`,
		``,
	}, "\n")
	result := classifyProbeStream(strings.NewReader(stream))
	if result.Outcome != probeSuccess || result.Text != "OK" || !result.Completed {
		t.Fatalf("final message duplicated streamed delta: %#v", result)
	}
}

func TestClassifyMalformedEventCannotBeWashedIntoSuccess(t *testing.T) {
	stream := "data: {bad json}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"OK\"}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
	result := classifyProbeStream(strings.NewReader(stream))
	if result.Outcome != probeInconclusive {
		t.Fatalf("malformed stream must not recover account: %#v", result)
	}
}

func TestClassifyEventOnlyFailure(t *testing.T) {
	result := classifyProbeStream(strings.NewReader("event: response.failed\n\n"))
	if result.Outcome != probeFailure {
		t.Fatalf("event-only failure must fail: %#v", result)
	}
}

func TestClassifyOversizedStreamCannotHideLateFailure(t *testing.T) {
	prefix := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"OK\"}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
	stream := prefix + strings.Repeat(": padding\n", 500000) + "event: response.failed\n\n"
	result := classifyProbeStream(strings.NewReader(stream))
	if result.Outcome != probeInconclusive {
		t.Fatalf("oversized stream must not succeed: %#v", result)
	}
}
