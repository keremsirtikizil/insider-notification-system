package migrations_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keremsirtikizil/insider-notification-system/internal/testutil"
	"github.com/keremsirtikizil/insider-notification-system/migrations"
)

// TestMigrationsRoundTrip proves every down migration reverses its up migration.
func TestMigrationsRoundTrip(t *testing.T) {
	pool := testutil.NewDatabase(t) // already migrated up
	dbURL := pool.Config().ConnString()

	version, dirty, err := migrations.Version(dbURL)
	require.NoError(t, err)
	assert.EqualValues(t, 3, version)
	assert.False(t, dirty)

	require.NoError(t, migrations.Down(dbURL, 3))
	version, _, err = migrations.Version(dbURL)
	require.NoError(t, err)
	assert.EqualValues(t, 0, version)
	var tables int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'schema_migrations'`).Scan(&tables))
	assert.Zero(t, tables, "down migrations leave no tables behind")

	require.NoError(t, migrations.Up(dbURL))
	require.NoError(t, migrations.Up(dbURL), "up is idempotent")
	version, _, err = migrations.Version(dbURL)
	require.NoError(t, err)
	assert.EqualValues(t, 3, version)
}
