package quota

import (
	"encoding/json"
	"testing"
)

func TestSyntheticPoolFromQuotaNoJSONCycle(t *testing.T) {
	item := map[string]any{
		"ok":         true,
		"account_id": "acc-1",
		"exhausted":  false,
		"source":     "billing",
	}
	pool := syntheticPoolFromQuota("acc-1", item)
	if pool == nil {
		t.Fatal("pool is nil")
	}
	if _, ok := pool["last_quota"]; ok {
		t.Fatal("synthetic pool must not embed last_quota (JSON cycle risk)")
	}
	item["pool"] = pool
	if _, err := json.Marshal(item); err != nil {
		t.Fatalf("marshal cyclic risk: %v", err)
	}

	// Exhausted path
	bad := map[string]any{
		"ok":             true,
		"exhausted":      true,
		"exhaust_reason": "quota empty",
		"source":         "billing",
	}
	pool2 := syntheticPoolFromQuota("acc-2", bad)
	bad["pool"] = pool2
	bad["auto_disabled"] = true
	if _, err := json.Marshal(bad); err != nil {
		t.Fatalf("exhausted marshal: %v", err)
	}
	if pool2["pool_status"] != "quota_disabled" {
		t.Fatalf("pool_status=%v", pool2["pool_status"])
	}
	if pool2["disabled_for_quota"] != true {
		t.Fatalf("disabled_for_quota=%v", pool2["disabled_for_quota"])
	}
}

func TestSyntheticPoolFromQuotaNilItem(t *testing.T) {
	pool := syntheticPoolFromQuota("acc-x", nil)
	if pool["id"] != "acc-x" {
		t.Fatalf("id=%v", pool["id"])
	}
	if _, err := json.Marshal(pool); err != nil {
		t.Fatal(err)
	}
}
