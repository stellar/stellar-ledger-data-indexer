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
	fmt.Println("opening postgres session")

	session, err := db.Open("postgres", connStr)

	if err != nil {
		return nil, fmt.Errorf("failed to open postgres instance: %w", err)
	}

	if err := session.Ping(ctx, 5*time.Second); err != nil {
		return nil, fmt.Errorf("failed to ping postgres instance: %w", err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		panic(fmt.Sprintf("Failed to fetch current directory: %v", err))
	}
	migrationsPath := filepath.Join(cwd, "internal", "db", "migrations")
	migrations := &migrate.FileMigrationSource{
		Dir: migrationsPath,
	}
	n, err := migrate.Exec(session.DB.DB, "postgres", migrations, migrate.Up)
	if err != nil {
		panic(fmt.Sprintf("Failed to apply migrations: %v", err))
	}
	fmt.Printf("Applied %d migrations!\n", n)

	return &Q{SessionInterface: session}, nil
}
