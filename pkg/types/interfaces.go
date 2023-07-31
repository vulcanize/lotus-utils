package types

import (
	"context"
	"io"
	"sync"
)

// Checksummer is the interface for the checksummer
type Checksummer interface {
	FindGaps(start, stop int) ([][2]uint, error)
	Checksum(start, stop uint) (string, error)
	CheckRangeIsPopulated(start, stop uint) (bool, error)
	io.Closer
}

// ChecksumRepository is the interface for the checksum repository
type ChecksumRepository interface {
	PublishChecksum(start, stop uint, hash string) error
	ChecksumExists(hash string) (bool, error)
	GetChecksum(start, stop uint) (string, error)
	FindNextChecksum() (uint, error)
	FindGaps(start, stop int) ([][2]uint, error)
	Interval() uint
	io.Closer
}

// GetChecksumRequest holds the arguments to `GetChecksum` since net/rpc only supports a single request argument
type GetChecksumRequest struct {
	Start uint
	Stop  uint
}

// API is the interface for the attestation service API
type API interface {
	ChecksumExists(hash string, res *bool) error
	GetChecksum(rng GetChecksumRequest, res *string) error
}

// AttestationService is the top-level interface for the attestation service
type AttestationService interface {
	Checksum(ctx context.Context, wg *sync.WaitGroup) (error, <-chan error)
	Serve(ctx context.Context, wg *sync.WaitGroup) error
	Register(reg func(any) error) error
	io.Closer
}
