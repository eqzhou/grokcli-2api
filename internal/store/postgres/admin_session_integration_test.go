package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestAdminSessionConcurrentCreatesPreserveEveryTokenIntegration(t *testing.T) {
	conn := testConnector(t)
	const count = 24
	prefix := fmt.Sprintf("admin-it-%d-", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = conn.Pool.Exec(context.Background(), `
			UPDATE app_settings
			SET value = COALESCE((
				SELECT jsonb_object_agg(entry.key, entry.value)
				FROM jsonb_each(value) AS entry
				WHERE entry.key NOT LIKE $1
			), '{}'::jsonb)
			WHERE key = 'sessions'
		`, prefix+"%")
	})

	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		token := fmt.Sprintf("%s%d", prefix, i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- conn.CreateAdminSession(token)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < count; i++ {
		token := fmt.Sprintf("%s%d", prefix, i)
		if !conn.VerifyAdminSession(token) {
			t.Fatalf("concurrent create lost token %q", token)
		}
	}
}
