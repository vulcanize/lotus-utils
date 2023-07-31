package attestation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/vulcanize/lotus-utils/pkg/types"
)

var _ types.AttestationService = (*Service)(nil)

// Service is the attestation service top-level object
type Service struct {
	cs                types.Checksummer
	r                 types.ChecksumRepository
	api               *API
	start             uint
	checksumChunkSize uint
	quit              chan struct{}
}

// NewServiceFromConfig creates a new attestation service from a config object
func NewServiceFromConfig(c *Config) (*Service, error) {
	var cs types.Checksummer
	var err error
	if c.Checksum {
		cs, err = NewChecksummer(c.SrcDBDir)
		if err != nil {
			return nil, err
		}
	}
	repo, existed, err := NewRepo(c.RepoDBDir, c.ChecksumChunkSize)
	if err != nil {
		return nil, err
	}
	var start uint
	if existed {
		start, err = repo.FindNextChecksum()
		if err != nil {
			return nil, err
		}
	}
	return &Service{cs: cs, r: repo, start: start, api: NewAPI(repo), quit: make(chan struct{}), checksumChunkSize: c.ChecksumChunkSize}, nil
}

// NewService creates a new attestation service
// it accepts pre-initialized checksummer and checksum repository objects
// useful for testing with mocks that satisfy these interfaces
func NewService(cs types.Checksummer, repo types.ChecksumRepository, start, chunkSize uint) (*Service, error) {
	if repo == nil {
		return nil, fmt.Errorf("cannot create attestation service without a checksum repository")
	}
	if chunkSize == 0 {
		chunkSize = defaultChecksumChunkSize
	}
	return &Service{cs: cs, r: repo, start: start, api: NewAPI(repo), quit: make(chan struct{}), checksumChunkSize: chunkSize}, nil
}

// Checksum starts the attestation service checksumming and publishing loop
func (s *Service) Checksum(ctx context.Context, wg *sync.WaitGroup) (error, <-chan error) {
	// TODO: have a mode for ongoing checksumming while a lotus node continues to process new blocks
	// TODO: and another mode that operates on offline database and the exits once it runs out of chunks to process
	if s.r == nil {
		return fmt.Errorf("cannot checksum without a checksum repository"), nil
	}
	if s.cs == nil {
		return fmt.Errorf("cannot checksum without a checksummer"), nil
	}
	// TODO: if c.CheckForGaps == true, check for gaps in the checksum repo and backfill them before starting
	wg.Add(1)
	start := s.start
	errChan := make(chan error)
	go func() {
		defer func() {
			logrus.Info("attestation service checksumming loop exited")
		}()
		defer wg.Done()
		for {
			select {
			case <-s.quit:
				return
			case <-ctx.Done():
				return
			default:
				// if the next range is not populated in the src msgindex db, do not continue
				stop := start + s.checksumChunkSize
				populated, err := s.cs.CheckRangeIsPopulated(start, stop)
				if err != nil {
					errChan <- err
					return
				}
				if !populated {
					// the range is incomplete, we need to wait to continue (or fall over, or trigger backfilling the index)
					// TODO: more sophisticated logic
					time.Sleep(30 * time.Second)
					continue
				}
				// it is populated, so calculate the checksum
				checksum, err := s.cs.Checksum(start, stop)
				if err != nil {
					errChan <- err
					return
				}
				// and publish it in the repository
				if err = s.r.PublishChecksum(start, stop, checksum); err != nil {
					errChan <- err
					return
				}
				// assign the next chunk start epoch and continue
				start = stop + 1
			}
		}
	}()
	return nil, errChan
}

// Serve starts an empty loop that waits for a quit signal
// used to isolate the RPC server loop from the checksum processing loop
// e.g. can start this with only a checksum repository to serve the RPC API, with no active background checksummer process
// or can use a cancel ctx passed into `Start` to stop the checksumming processes without stopping this loop
// TODO: wire the API into here such that
// 1. users can request a (missing) checksum be calculated
// 2. if a request to GetChecksum is made for a range that is not yet checksummed, the checksumming process can be triggered (if the range is found to be complete in local msgindex.db)
func (s *Service) Serve(ctx context.Context, wg *sync.WaitGroup) error {
	if s.r == nil {
		return fmt.Errorf("cannot serve without a checksum repository")
	}
	wg.Add(1)
	go func() {
		defer func() {
			logrus.Info("attestation service serve loop exited")
		}()
		defer wg.Done()
		for {
			select {
			case <-s.quit:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// Register registers the internal API with the provided registration function (e.g. rpc.Register)
func (s *Service) Register(reg func(any) error) error {
	return reg(s.api)
}

// Close implements io.Closer
// it shuts down any active Checksum or Serve loops
func (s *Service) Close() error {
	if err := s.cs.Close(); err != nil {
		return err
	}
	return s.r.Close()
}
