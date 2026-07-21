package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const adminSessionTTL = 7 * 24 * time.Hour

func (c *Connector) VerifyAdminSession(token string) bool {
	token = strings.TrimSpace(token)
	if token == "" || c == nil || c.Pool == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var data []byte
	if err := c.Pool.QueryRow(ctx, "SELECT value FROM app_settings WHERE key = 'sessions'").Scan(&data); err != nil {
		return false
	}
	var sessions map[string]any
	if err := json.Unmarshal(data, &sessions); err != nil || sessions == nil {
		return false
	}
	raw, ok := sessions[token]
	if !ok {
		return false
	}
	ts, ok := toFloat(raw)
	if !ok || time.Since(time.Unix(int64(ts), 0)) > adminSessionTTL {
		return false
	}
	return true
}

func toFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int64:
		return float64(v), true
	case int:
		return float64(v), true
	default:
		return 0, false
	}
}

func (c *Connector) CreateAdminSession(token string) error {
	token = strings.TrimSpace(token)
	if token == "" || c == nil || c.Pool == nil {
		return errors.New("postgres admin session store unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	now := time.Now().Unix()
	cutoff := now - int64(adminSessionTTL/time.Second)
	_, err := c.Pool.Exec(ctx, `
		INSERT INTO app_settings (key, value, updated_at)
		VALUES ('sessions', jsonb_build_object($1::text, $2::bigint), now())
		ON CONFLICT (key) DO UPDATE SET
			value = (
				SELECT COALESCE(jsonb_object_agg(entry.key, entry.value), '{}'::jsonb)
				FROM jsonb_each(
					CASE WHEN jsonb_typeof(app_settings.value) = 'object'
						THEN app_settings.value ELSE '{}'::jsonb END
				) AS entry
				WHERE jsonb_typeof(entry.value) = 'number'
				  AND (entry.value #>> '{}')::double precision >= $3::double precision
			) || jsonb_build_object($1::text, $2::bigint),
			updated_at = now()
	`, token, now, cutoff)
	return err
}

func (c *Connector) DeleteAdminSession(token string) error {
	token = strings.TrimSpace(token)
	if token == "" || c == nil || c.Pool == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := c.Pool.Exec(ctx, `
		UPDATE app_settings
		SET value = CASE WHEN jsonb_typeof(value) = 'object'
			THEN value - $1::text ELSE '{}'::jsonb END,
			updated_at = now()
		WHERE key = 'sessions'
	`, token)
	return err
}

func (c *Connector) DeleteAllAdminSessions() error {
	if c == nil || c.Pool == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := c.Pool.Exec(ctx, `
		INSERT INTO app_settings (key, value, updated_at)
		VALUES ('sessions', '{}'::jsonb, now())
		ON CONFLICT (key) DO UPDATE SET value = '{}'::jsonb, updated_at = now()
	`)
	return err
}
