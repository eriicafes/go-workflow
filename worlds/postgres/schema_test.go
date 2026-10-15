package postgresworld

import (
	"context"
	"strings"
	"testing"
)

func TestMigrate(t *testing.T) {
	t.Parallel()
	url, schema := testSchema(t)
	pool := testPool(t, url, schema)
	ctx := context.Background()
	for range 2 {
		if err := migrate(ctx, pool); err != nil {
			t.Fatal(err)
		}
	}
	var count, version int
	if err := pool.QueryRow(ctx, `SELECT count(*), max(version) FROM go_workflow_schema_version`).Scan(&count, &version); err != nil {
		t.Fatal(err)
	}
	if count != len(migrations) || version != len(migrations) {
		t.Fatalf("versions = %d max %d, want %d", count, version, len(migrations))
	}
	if _, err := pool.Exec(ctx, `INSERT INTO go_workflow_schema_version (version) VALUES ($1)`, len(migrations)+1); err != nil {
		t.Fatal(err)
	}
	if err := migrate(ctx, pool); err == nil || !strings.Contains(err.Error(), "newer") {
		t.Fatalf("migrate over newer schema err = %v", err)
	}
}
