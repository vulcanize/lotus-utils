package attestation

import (
	"fmt"

	"github.com/vulcanize/lotus-utils/pkg/types"
)

var _ types.API = (*API)(nil)

// API is kind of an unnecessary abstraction since this only wraps a single backing struct, but it will make it easier
// to extend the API in the future (add and use new backing components)
type API struct {
	backend types.ChecksumRepository
}

// NewAPI returns a new API object
func NewAPI(repo types.ChecksumRepository) *API {
	return &API{backend: repo}
}

// ChecksumExists returns true if the given checksum is published in the backing checksum repository
func (a API) ChecksumExists(hash string, res *bool) error {
	exists, err := a.backend.ChecksumExists(hash)
	if err != nil {
		return err
	}
	*res = exists
	return nil
}

// GetChecksum returns the checksum for the given start and stop values
func (a API) GetChecksum(rng types.GetChecksumRequest, res *string) error {
	if rng.Stop-rng.Stop != a.backend.Interval() {
		return fmt.Errorf("checksum expected to span an interval of size %d", a.backend.Interval())
	}
	if rng.Stop%a.backend.Interval() != 0 {
		return fmt.Errorf("checksum range must start at a multiple of the interval size %d", a.backend.Interval())
	}
	hash, err := a.backend.GetChecksum(rng.Start, rng.Stop)
	if err != nil {
		return err
	}
	*res = hash
	return nil
}
