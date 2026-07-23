-- Durable state for low-frequency, concurrency-safe account recovery probes.
-- admin_locked is an authorization boundary: automated recovery must never
-- re-enable a row while an administrator explicitly holds it out of rotation.

ALTER TABLE account_pool
  ADD COLUMN IF NOT EXISTS admin_locked BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE account_pool
  ADD COLUMN IF NOT EXISTS recovery_next_probe_at TIMESTAMPTZ;
ALTER TABLE account_pool
  ADD COLUMN IF NOT EXISTS recovery_fail_count INT NOT NULL DEFAULT 0;
ALTER TABLE account_pool
  ADD COLUMN IF NOT EXISTS recovery_lease_until TIMESTAMPTZ;
ALTER TABLE account_pool
  ADD COLUMN IF NOT EXISTS last_recovery_probe_at TIMESTAMPTZ;
ALTER TABLE account_pool
  ADD COLUMN IF NOT EXISTS last_recovery_outcome TEXT;

-- Preserve only explicit, currently-active manual whole-account decisions.
-- Historical disabled rows without provenance intentionally remain recoverable.
UPDATE account_pool
SET admin_locked = true
WHERE admin_locked = false
  AND COALESCE(extra->>'manual_status', '') IN ('disabled', 'quota_disabled')
  AND COALESCE(pool_status, '') = COALESCE(extra->>'manual_status', '');

CREATE INDEX IF NOT EXISTS idx_account_pool_recovery_due
  ON account_pool (recovery_next_probe_at NULLS FIRST, account_id)
  WHERE admin_locked = false AND (enabled = false OR disabled_for_quota = true);
