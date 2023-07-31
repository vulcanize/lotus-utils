package attestation

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"

	"github.com/vulcanize/lotus-utils/pkg/types"
)

var _ types.ChecksumRepository = (*Repo)(nil)

var (
	repoDBName               = "checksums.db"
	checkSumExistsStmt       = "SELECT EXISTS(SELECT 1 FROM checksums WHERE hash = ?)"
	insertCheckSumStmt       = "INSERT INTO checksums (start, stop, hash) VALUES (?, ?, ?)"
	getChecksumForRangeStmt  = "SELECT hash FROM checksums where start = ? AND stop = ?"
	findLatestCheckSumStmt   = "SELECT stop FROM checksums ORDER BY stop DESC LIMIT 1"
	findChecksumGapsBaseStmt = "SELECT start as first_missing, (next_start-1) as last_missing from " +
		"(SELECT start, stop, LEAD(start) OVER (ORDER BY start) AS next_start) h WHERE next_start > stop + 1"
	findChecksumGapsBaseStmt2 = "SELECT start + ? AS first_missing, (next_nc - ?) AS last_missing " +
		"FROM (SELECT start, LEAD(start) OVER (ORDER BY start) AS next_nc FROM checksums %s) h " +
		"WHERE next_nc > start + ?"
	defaultChecksumChunkSize uint = 2880
)

var repoDBDefs = []string{
	`CREATE TABLE IF NOT EXISTS checksums (
     hash VARCHAR(66) PRIMARY KEY ON CONFLICT REPLACE,
     start INTEGER NOT NULL,
     stop INTEGER NOT NULL,
	 UNIQUE (start, stop) ON CONFLICT REPLACE
   )`,
	`CREATE INDEX IF NOT EXISTS checksum_hashes ON checksums (hash)`,
	`CREATE INDEX IF NOT EXISTS checksum_starts ON checksums (start)`,
	`CREATE INDEX IF NOT EXISTS checksum_stops ON checksums (stop)`,
}

type Repo struct {
	repoDB   *sql.DB
	interval uint
}

// NewRepo creates a new checksum repository object
func NewRepo(repoDir string, interval uint) (*Repo, bool, error) {
	if interval == 0 {
		interval = defaultChecksumChunkSize
	}
	var existed bool
	repoDBPath := filepath.Join(repoDir, repoDBName)
	_, err := os.Stat(repoDBPath)
	switch {
	case err == nil:
		existed = true

	case errors.Is(err, fs.ErrNotExist):

	case err != nil:
		return nil, false, xerrors.Errorf("error stating src msgindex database: %w", err)
	}

	repoDB, err := sql.Open("sqlite3", repoDBPath+"?mode=rwc")
	if err != nil {
		return nil, existed, xerrors.Errorf("open sqlite3 database: %w", err)
	}
	for _, stmt := range repoDBDefs {
		_, err = repoDB.Exec(stmt)
		if err != nil {
			return nil, existed, xerrors.Errorf("create checksum db schema (stmt: %s): %w", stmt, err)
		}
	}
	return &Repo{repoDB: repoDB, interval: interval}, existed, nil
}

// PublishChecksum publishes the given checksum hash for the given range
func (r *Repo) PublishChecksum(start, stop uint, hash string) error {
	_, err := r.repoDB.Exec(insertCheckSumStmt, start, stop, hash)
	return err
}

// ChecksumExists checks if the given checksum hash exists in the repository
func (r *Repo) ChecksumExists(hash string) (bool, error) {
	var exists bool
	return false, r.repoDB.QueryRow(checkSumExistsStmt, hash).Scan(&exists)
}

// GetChecksum gets the checksum for the given range
func (r *Repo) GetChecksum(start, stop uint) (string, error) {
	var hash string
	err := r.repoDB.QueryRow(getChecksumForRangeStmt, start, stop).Scan(&hash)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return hash, err
}

// FindNextChecksum finds the `start` epoch for the next checksum that needs to be published
func (r *Repo) FindNextChecksum() (uint, error) {
	var lastStop uint
	err := r.repoDB.QueryRow(findLatestCheckSumStmt).Scan(&lastStop)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return lastStop + 1, err
}

// FindGaps finds gaps in the checksums table between start and stop (inclusive)
func (r *Repo) FindGaps(start, stop int) ([][2]uint, error) {
	var where string
	if start >= 0 && stop >= 0 && start <= stop {
		where = fmt.Sprintf("WHERE epoch >= %d AND epoch <= %d", start, stop)
	} else if start >= 0 {
		where = fmt.Sprintf("WHERE epoch >= %d", start)
	} else if stop >= 0 {
		where = fmt.Sprintf("WHERE epoch <= %d", stop)
	}
	rows, err := r.repoDB.Query(fmt.Sprintf(findChecksumGapsBaseStmt2, where), r.interval, r.interval, r.interval)
	if err != nil {
		if err == sql.ErrNoRows {
			logrus.Infof("No gaps found for range %d to %d", start, stop)
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	var gaps [][2]uint
	for rows.Next() {
		var gapStart, gapStop uint
		if err := rows.Scan(&gapStart, &gapStop); err != nil {
			return nil, err
		}
		gaps = append(gaps, [2]uint{gapStart, gapStop})
	}
	return gaps, nil
}

// Close implements io.Closer
func (r *Repo) Close() error {
	return r.repoDB.Close()
}

// Interval returns the checksum interval used for this repo
func (r *Repo) Interval() uint {
	return r.interval
}
