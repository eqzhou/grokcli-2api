package modelhealth

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"
)

type probeOutcome string

const (
	probeSuccess      probeOutcome = "success"
	probeFailure      probeOutcome = "failure"
	probeInconclusive probeOutcome = "inconclusive"
)

type probeStreamResult struct {
	Outcome      probeOutcome
	Text         string
	Completed    bool
	Terminal     string
	ErrorType    string
	ErrorCode    string
	ErrorMessage string
	Malformed    int
}

// classifyProbeStream consumes raw Responses or Chat Completions SSE. A probe is
// successful only when it contains non-empty assistant text and a normal protocol
// terminal. [DONE] alone is only a transport marker and never proves model health.
func classifyProbeStream(reader io.Reader) probeStreamResult {
	result := probeStreamResult{Outcome: probeInconclusive, Terminal: "eof"}
	if reader == nil {
		return result
	}

	limited := &io.LimitedReader{R: reader, N: (4 << 20) + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 32<<10), 1<<20)
	eventName := ""
	dataLines := make([]string, 0, 2)
	textParts := make([]string, 0, 2)
	explicitFailure := false

	flush := func() {
		if len(dataLines) == 0 {
			hint := strings.ToLower(strings.TrimSpace(eventName))
			if isProbeFailureEvent(hint) {
				explicitFailure = true
				result.Terminal = hint
				result.ErrorType = hint
			}
			eventName = ""
			return
		}
		joined := strings.TrimSpace(strings.Join(dataLines, "\n"))
		dataLines = dataLines[:0]
		if joined == "[DONE]" {
			if result.Terminal == "eof" {
				result.Terminal = "done"
			}
			eventName = ""
			return
		}

		var payload map[string]any
		if err := json.Unmarshal([]byte(joined), &payload); err != nil {
			result.Malformed++
			eventName = ""
			return
		}
		typeName := strings.ToLower(strings.TrimSpace(stringAny(payload["type"])))
		if typeName == "" {
			typeName = strings.ToLower(strings.TrimSpace(eventName))
		}
		if isProbeFailureEvent(typeName) || payload["error"] != nil {
			explicitFailure = true
			result.Terminal = firstNonEmptyProbe(typeName, "error")
			captureProbeError(&result, payload)
		}

		switch typeName {
		case "response.output_text.delta":
			if delta := strings.TrimSpace(stringAny(payload["delta"])); delta != "" {
				textParts = append(textParts, delta)
			}
		case "response.completed":
			response, _ := payload["response"].(map[string]any)
			if response == nil {
				result.Malformed++
				break
			}
			status := strings.ToLower(strings.TrimSpace(stringAny(response["status"])))
			if status != "completed" {
				if status == "" {
					result.Malformed++
					break
				}
				explicitFailure = true
				result.Terminal = "response." + status
				captureProbeError(&result, response)
			} else {
				result.Completed = true
				result.Terminal = "completed"
				textParts = append(textParts, completedOutputText(response["output"])...)
			}
		case "response.failed", "response.error", "response.incomplete", "error":
			explicitFailure = true
		}

		if choices, ok := payload["choices"].([]any); ok {
			for _, rawChoice := range choices {
				choice, _ := rawChoice.(map[string]any)
				if choice == nil {
					continue
				}
				for _, key := range []string{"delta", "message"} {
					part, _ := choice[key].(map[string]any)
					if content := strings.TrimSpace(stringAny(part["content"])); content != "" {
						textParts = append(textParts, content)
					}
				}
				finish := strings.ToLower(strings.TrimSpace(stringAny(choice["finish_reason"])))
				if finish == "stop" {
					result.Completed = true
					result.Terminal = "completed"
				} else if finish != "" {
					explicitFailure = true
					result.Terminal = "finish_" + finish
					result.ErrorType = "finish_" + finish
				}
			}
		}
		eventName = ""
	}

	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		if line == "" {
			flush()
			continue
		}
		if strings.HasPrefix(line, "event:") {
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}
		if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	flush()
	if err := scanner.Err(); err != nil {
		result.ErrorType = "stream_read_error"
		result.ErrorMessage = err.Error()
		result.Completed = false
	}
	if limited.N <= 0 {
		result.Malformed++
		result.ErrorType = "stream_size_limit"
		result.ErrorMessage = "probe SSE stream exceeded size limit"
		result.Completed = false
	}

	result.Text = strings.TrimSpace(strings.Join(textParts, ""))
	switch {
	case explicitFailure:
		result.Outcome = probeFailure
	case result.Malformed > 0:
		result.Outcome = probeInconclusive
	case result.Text != "" && result.Completed:
		result.Outcome = probeSuccess
	default:
		result.Outcome = probeInconclusive
	}
	return result
}

func isProbeFailureEvent(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "response.failed", "response.error", "response.incomplete", "error":
		return true
	default:
		return false
	}
}

func captureProbeError(result *probeStreamResult, payload map[string]any) {
	if result == nil || payload == nil {
		return
	}
	result.ErrorType = firstNonEmptyProbe(result.ErrorType, stringAny(payload["type"]))
	for _, candidate := range []map[string]any{
		mapAny(payload["error"]),
		mapAny(mapAny(payload["response"])["error"]),
		payload,
	} {
		if candidate == nil {
			continue
		}
		result.ErrorType = firstNonEmptyProbe(result.ErrorType, stringAny(candidate["type"]))
		result.ErrorCode = firstNonEmptyProbe(result.ErrorCode, stringAny(candidate["code"]))
		result.ErrorMessage = firstNonEmptyProbe(result.ErrorMessage, stringAny(candidate["message"]))
	}
}

func completedOutputText(value any) []string {
	out := []string{}
	var walk func(any)
	walk = func(node any) {
		switch v := node.(type) {
		case []any:
			for _, item := range v {
				walk(item)
			}
		case map[string]any:
			typeName := strings.ToLower(strings.TrimSpace(stringAny(v["type"])))
			if (typeName == "output_text" || typeName == "text") && strings.TrimSpace(stringAny(v["text"])) != "" {
				out = append(out, strings.TrimSpace(stringAny(v["text"])))
			}
			for _, key := range []string{"output", "content"} {
				walk(v[key])
			}
		}
	}
	walk(value)
	return out
}

func mapAny(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func stringAny(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func firstNonEmptyProbe(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func truncateProbeText(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit]
}
