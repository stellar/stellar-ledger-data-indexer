package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	migrate "github.com/rubenv/sql-migrate"
	"github.com/stellar/go/support/db"
)

func NewPostgresSession(ctx context.Context, connStr string) (*Q, error) {
	session, err := db.Open("postgres", connStr)

	if err != nil {
		return nil, fmt.Errorf("failed to open postgres instance: %w", err)
	}

	if err := session.Ping(ctx, 5*time.Second); err != nil {
		return nil, fmt.Errorf("failed to ping postgres instance: %w", err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("Failed to fetch current directory: %w", err)
	}
	migrationsPath := filepath.Join(cwd, "internal", "db", "migrations")
	migrations := &migrate.FileMigrationSource{
		Dir: migrationsPath,
	}
	_, err = migrate.Exec(session.DB.DB, "postgres", migrations, migrate.Up)
	if err != nil {
		return nil, fmt.Errorf("Failed to apply migrations: %v", err)
	}

	return &Q{SessionInterface: session}, nil
}
