package postgres

import "testing"

func TestLastProbeStatusUsesExplicitTriState(t *testing.T) {
	tests := []struct {
		probe map[string]any
		want  string
	}{
		{map[string]any{"outcome": "success", "available": false}, "ok"},
		{map[string]any{"outcome": "failure", "available": true}, "fail"},
		{map[string]any{"outcome": "inconclusive", "available": false}, "inconclusive"},
		{map[string]any{"probe_status": "inconclusive", "available": true}, "inconclusive"},
		{map[string]any{"available": true}, "ok"},
		{map[string]any{"available": false}, "fail"},
	}
	for _, tc := range tests {
		if got := lastProbeStatus(tc.probe); got != tc.want {
			t.Fatalf("lastProbeStatus(%#v)=%q want %q", tc.probe, got, tc.want)
		}
	}
}
