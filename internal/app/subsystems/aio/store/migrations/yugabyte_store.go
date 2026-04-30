package migrations

import (
	"database/sql"
	"fmt"

	"github.com/resonatehq/resonate/internal/migrationfiles"
)

// YugabyteMigrationStore implements MigrationStore for YugaByteDB databases
type YugabyteMigrationStore struct {
	db *sql.DB
}

// NewYugabyteMigrationStore creates a new YugaByteDB migration store
func NewYugabyteMigrationStore(db *sql.DB) *YugabyteMigrationStore {
	return &YugabyteMigrationStore{db: db}
}

func (s *YugabyteMigrationStore) GetMigrationFiles() ([]string, error) {
	return migrationfiles.GetMigrationFiles("migrations/yugabyte")
}

func (s *YugabyteMigrationStore) GetMigrationContent(path string) (string, error) {
	return migrationfiles.GetMigrationContent(path)
}

func (s *YugabyteMigrationStore) GetInsertMigrationSQL() string {
	return "INSERT INTO migrations (id) VALUES ($1) ON CONFLICT(id) DO NOTHING"
}

func (s *YugabyteMigrationStore) GetCurrentVersion() (int, error) {
	var version int
	err := s.db.QueryRow("SELECT id FROM migrations ORDER BY id DESC LIMIT 1").Scan(&version)

	// We create the migrations table on store start, we can assume that ErrNoRows means that no migrations have been applied
	// and the db is fresh
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get current migration version: %w", err)
	}
	return version, nil
}

func (s *YugabyteMigrationStore) GetDB() *sql.DB {
	return s.db
}

func (s *YugabyteMigrationStore) CheckMigrations() error {
	currentVersion, err := s.GetCurrentVersion()
	if err != nil {
		return err
	}

	pending, err := GetPendingMigrations(currentVersion, s)
	if err != nil {
		return err
	}

	if len(pending) > 0 {
		return &MigrationError{
			Version: pending[0].Version,
			Name:    pending[0].Name,
			Err:     ErrPendingMigrations,
		}
	}

	return nil
}

func (s *YugabyteMigrationStore) String() string {
	return "yugabyte"
}

func (s *YugabyteMigrationStore) Close() error {
	return s.db.Close()
}
