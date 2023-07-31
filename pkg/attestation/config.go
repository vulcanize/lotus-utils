package attestation

import (
	"errors"

	"github.com/spf13/viper"
)

// Env variables
const (
	LOG_FILE  = "LOG_FILE"
	LOG_LEVEL = "LOG_LEVEL"

	SERVER_PORT     = "SERVER_PORT"
	SUPPORTS_SERVER = "SUPPORTS_SERVER"

	CHECKSUM_DB_DIRECTORY  = "CHECKSUM_DB_DIRECTORY"
	MSG_INDEX_DB_DIRECTORY = "MSG_INDEX_DB_DIRECTORY"

	SUPPORTS_CHECKSUMMING = "SUPPORTS_CHECKSUMMING"
	CHECKSUM_CHUNK_SIZE   = "CHECKSUM_CHUNK_SIZE"
)

// TOML bindings
const (
	LOG_FILE_TOML  = "log.file"
	LOG_LEVEL_TOML = "log.level"

	SERVER_PORT_TOML     = "server.port"
	SUPPORTS_SERVER_TOML = "server.on"

	CHECKSUM_DB_DIRECTORY_TOML  = "database.checksumPath"
	MSG_INDEX_DB_DIRECTORY_TOML = "database.msgIndexPath"

	SUPPORTS_CHECKSUMMING_TOML = "checksum.on"
	CHECKSUM_CHUNK_SIZE_TOML   = "checksum.chunkSize"
)

// Config holds the configuration params for the attestation service
type Config struct {
	// support checksumming
	Checksum bool
	// support API
	Serve bool
	// Port to expose API on
	ServerPort string
	// Directory with the source msgindex.db sqlite file
	SrcDBDir string
	// Directory with/for the checksums.db sqlite file
	RepoDBDir string
	// Chunk range size for checksumming
	ChecksumChunkSize uint
	// Whether to check for gaps in the checksum repo at initialization if the repo already exists
	CheckForGaps uint
}

// NewConfig is used to initialize a watcher config from a .toml file
// Separate chain watcher instances need to be ran with separate ipfs path in order to avoid lock contention on the ipfs repository lockfile
func NewConfig() (*Config, error) {
	c := new(Config)

	viper.BindEnv(SERVER_PORT_TOML, SERVER_PORT)
	viper.BindEnv(SUPPORTS_SERVER_TOML, SUPPORTS_SERVER)

	viper.BindEnv(CHECKSUM_DB_DIRECTORY_TOML, CHECKSUM_DB_DIRECTORY)
	viper.BindEnv(MSG_INDEX_DB_DIRECTORY_TOML, MSG_INDEX_DB_DIRECTORY)
	viper.BindEnv(SUPPORTS_CHECKSUMMING_TOML, SUPPORTS_CHECKSUMMING)
	viper.BindEnv(CHECKSUM_CHUNK_SIZE_TOML, CHECKSUM_CHUNK_SIZE)

	checksummingEnabled := viper.GetBool(SUPPORTS_CHECKSUMMING_TOML)
	if checksummingEnabled {
		msgIndexDirPath := viper.GetString(MSG_INDEX_DB_DIRECTORY_TOML)
		if msgIndexDirPath == "" {
			return nil, errors.New("if checksumming is enabled, a source msgindex.db directoy path must be provided")
		}
		c.SrcDBDir = msgIndexDirPath
	}
	c.Checksum = checksummingEnabled

	checksumDBDirPath := viper.GetString(CHECKSUM_DB_DIRECTORY_TOML)
	if checksumDBDirPath == "" {
		return nil, errors.New("a checksums.db directory path must be provided")
	}
	c.RepoDBDir = checksumDBDirPath

	checksumChunkSize := viper.GetUint(CHECKSUM_CHUNK_SIZE_TOML)
	if checksumChunkSize == 0 {
		checksumChunkSize = defaultChecksumChunkSize
	}
	c.ChecksumChunkSize = checksumChunkSize

	// http server
	serverEnabled := viper.GetBool(SUPPORTS_SERVER_TOML)
	if serverEnabled {
		c.ServerPort = viper.GetString(SERVER_PORT_TOML)
		if c.ServerPort == "" {
			c.ServerPort = "8087"
		}
		c.Serve = serverEnabled
	}

	return c, nil
}
