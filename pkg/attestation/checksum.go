package attestation

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"

	"github.com/vulcanize/lotus-utils/pkg/types"
)

var _ types.Checksummer = (*CheckSummer)(nil)

type CheckSummer struct {
	tempDir   string
	srcDB     *sql.DB
	srcDBPath string
}

var (
	messagesDB               = "msgindex.db"
	msgIndexMigrateStmt      = "INSERT INTO messages SELECT * FROM src.messages WHERE epoch >= ? AND epoch =< ?"
	sha3sumDotCommand        = ".sha3sum"
	findMsgIndexGapsBaseStmt = "SELECT epoch + 1 AS first_missing, (next_nc - 1) AS last_missing " +
		"FROM (SELECT epoch, LEAD(epoch) OVER (ORDER BY epoch) AS next_nc FROM src.messages %s) h " +
		"WHERE next_nc > epoch + 1"
	doesEpochExistStmt            = "SELECT EXISTS(SELECT 1 FROM src.messages WHERE epoch = ?)"
	checkRangeIsPopulatedBaseStmt = fmt.Sprintf("SELECT EXISTS(%s)", findMsgIndexGapsBaseStmt)
)

// from lotus chain/store/sqlite/msgindex.go
var msgIndexDBDefs = []string{
	`CREATE TABLE IF NOT EXISTS messages (
     cid VARCHAR(80) PRIMARY KEY ON CONFLICT REPLACE,
     tipset_cid VARCHAR(80) NOT NULL,
     epoch INTEGER NOT NULL
   )`,
	`CREATE INDEX IF NOT EXISTS tipset_cids ON messages (tipset_cid)`,
	`CREATE INDEX IF NOT EXISTS tipset_epochs ON messages (epoch)`,
	`CREATE TABLE IF NOT EXISTS _meta (
    	version UINT64 NOT NULL UNIQUE
	)`,
	`INSERT OR IGNORE INTO _meta (version) VALUES (1)`,
}

// NewChecksummer creates a new checksumming object
func NewChecksummer(srcDir string) (*CheckSummer, error) {
	if srcDir == "" {
		return nil, xerrors.Errorf("checksummer srcDir path cannot be empty")
	}
	// Create a temporary directory to hold the SQLite database file
	tempDir, err := os.MkdirTemp("", "temp_db")
	if err != nil {
		return nil, err
	}

	srcDBPath := filepath.Join(srcDir, messagesDB)
	srcDB, err := sql.Open("sqlite3", srcDBPath+"?mode=rwc")
	if err != nil {
		return nil, err
	}
	return &CheckSummer{
		srcDBPath: filepath.Join(srcDir, messagesDB),
		srcDB:     srcDB,
		tempDir:   tempDir,
	}, nil
}

// Checksum checksums a chunk defined by the start and stop epochs (inclusive)
// this method assumes there are no gaps, so use the FindGaps first beforehand if we can't rely on another guarantee
func (cs *CheckSummer) Checksum(start, stop uint) (string, error) {
	// Create the path for the temporary database file
	tempDBPath := filepath.Join(cs.tempDir, messagesDB)
	dstMsgDB, err := sql.Open("sqlite3", tempDBPath+"?mode=rwc")
	if err != nil {
		return "", xerrors.Errorf("open sqlite3 database: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDBPath); err != nil {
			logrus.Errorf("remove temp db: %s", err)
		}
	}()
	defer func() {
		if err := dstMsgDB.Close(); err != nil {
			logrus.Errorf("close temp db: %s", err)
		}
	}()

	// Create the temporary dst messages table
	for _, stmt := range msgIndexDBDefs {
		_, err = dstMsgDB.Exec(stmt)
		if err != nil {
			return "", xerrors.Errorf("create temp msgindex schema (stmt: %s): %w", stmt, err)
		}
	}

	_, err = dstMsgDB.Exec("ATTACH DATABASE ? AS src", cs.srcDBPath)
	if err != nil {
		return "", xerrors.Errorf("attach src database: %w", err)
	}
	_, err = dstMsgDB.Exec(msgIndexMigrateStmt, start, stop)
	if err != nil {
		return "", xerrors.Errorf("migrate into dst.messages: %w", err)
	}
	_, err = dstMsgDB.Exec("DETACH src")
	if err != nil {
		return "", xerrors.Errorf("detach src database: %w", err)
	}
	var hash string
	return hash, dstMsgDB.QueryRow(sha3sumDotCommand).Scan(&hash)
}

// CheckRangeIsPopulated checks if the message index table is populated for the given range
func (cs *CheckSummer) CheckRangeIsPopulated(start, stop uint) (bool, error) {
	if start > stop {
		return false, xerrors.Errorf("start epoch cannot be greater than stop epoch")
	}
	// start by making the sure the `start` and `stop` epochs both exist in the database
	tx, err := cs.srcDB.Begin()
	if err != nil {
		return false, err
	}

	var startExists bool
	err = tx.QueryRow(doesEpochExistStmt, start).Scan(&startExists)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			logrus.Errorf("rollback error: %s", err.Error())
		}
		return false, err
	}
	var stopExists bool
	err = tx.QueryRow(doesEpochExistStmt, stop).Scan(&stopExists)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			logrus.Errorf("rollback error: %s", err.Error())
		}
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, xerrors.Errorf("commit error: %s", err.Error())
	}
	if !startExists || !stopExists {
		return false, nil
	}

	// if they do, check that the full range is populated
	where := fmt.Sprintf("WHERE epoch >= %d AND epoch <= %d", start, stop)
	var exists bool
	if err := cs.srcDB.QueryRow(fmt.Sprintf(checkRangeIsPopulatedBaseStmt, where)).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// FindGaps finds the gaps in the message index table
func (cs *CheckSummer) FindGaps(start, stop int) ([][2]uint, error) {
	var where string
	if start >= 0 && stop >= 0 && start <= stop {
		where = fmt.Sprintf("WHERE epoch >= %d AND epoch <= %d", start, stop)
	} else if start >= 0 {
		where = fmt.Sprintf("WHERE epoch >= %d", start)
	} else if stop >= 0 {
		where = fmt.Sprintf("WHERE epoch <= %d", stop)
	}
	rows, err := cs.srcDB.Query(fmt.Sprintf(findMsgIndexGapsBaseStmt, where))
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
func (cs *CheckSummer) Close() error {
	return os.RemoveAll(cs.tempDir)
}
