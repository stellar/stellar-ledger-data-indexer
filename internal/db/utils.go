package db

import (
	"context"
	"embed"
	"fmt"
	"time"

	migrate "github.com/rubenv/sql-migrate"
	"github.com/stellar/go/support/db"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func NewPostgresSession(ctx context.Context, connStr string) (*DBSession, error) {
	session, err := db.Open("postgres", connStr)

	if err != nil {
		return nil, fmt.Errorf("failed to open postgres instance: %w", err)
	}

	if err := session.Ping(ctx, 5*time.Second); err != nil {
		return nil, fmt.Errorf("failed to ping postgres instance: %w", err)
	}

	migrations := &migrate.EmbedFileSystemMigrationSource{
		FileSystem: migrationsFS,
		Root:       "internal/db/migrations",
	}
	_, err = migrate.Exec(session.DB.DB, "postgres", migrations, migrate.Up)
	if err != nil {
		return nil, fmt.Errorf("failed to apply migrations: %w", err)
	}

	return &DBSession{SessionInterface: session}, nil
}
