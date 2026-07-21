-- API clients authenticate against key_hash; retaining the raw key only turns a
-- read-only database or backup leak into immediate credential compromise.
UPDATE api_keys SET secret = NULL WHERE secret IS NOT NULL;
