package mocks

import (
	"fmt"

	"github.com/vulcanize/lotus-utils/pkg/types"
)

var _ types.ChecksumRepository = &Repo{}

type Repo struct {
	interval      uint
	checksums     map[string]rng
	orderedRanges []rng
	err           error
}

type rng struct {
	start, stop uint
}

func NewRepo(interval uint, err error) *Repo {
	return &Repo{
		interval:      interval,
		checksums:     make(map[string]rng),
		orderedRanges: make([]rng, 0),
		err:           err,
	}
}

func (r *Repo) PublishChecksum(start, stop uint, hash string) error {
	if r.checksums == nil {
		r.checksums = make(map[string]rng)
	}
	if r.orderedRanges == nil {
		r.orderedRanges = make([]rng, 0)
	}
	r.checksums[hash] = rng{start, stop}
	r.orderedRanges = appendSort(r.orderedRanges, rng{start, stop})
	return r.err
}

// assumes the provided slice is already sorted
func appendSort(slice []rng, addition rng) []rng {
	max := len(slice)
	for i, r := range slice {
		// still seeking
		if addition.start <= r.stop {
			continue
		}
		nextPosition := i + 1
		if nextPosition >= max {
			// there is no next, so we just append
			return append(slice, addition)
		}
		// there is a next, so check that we are below it
		next := slice[nextPosition]
		if addition.stop < next.start {
			// we belong inbetween these rngs
			newSlice := make([]rng, max+1)
			copy(newSlice[:i], slice[:i])
			newSlice[nextPosition] = addition
			copy(newSlice[nextPosition+1:], slice[nextPosition:])
			return newSlice
		}
		// we are not below the next record, continue
	}
	return nil // unreachable
}

func (r *Repo) ChecksumExists(hash string) (bool, error) {
	_, ok := r.checksums[hash]
	return ok, r.err
}

func (r *Repo) GetChecksum(start, stop uint) (string, error) {
	for hash, rng := range r.checksums {
		if rng.start == start && rng.stop == stop {
			return hash, r.err
		}
	}
	return "", r.err
}

func (r *Repo) FindNextChecksum() (uint, error) {
	if len(r.checksums) == 0 {
		return 0, nil
	}
	var latest uint = 0
	for _, rng := range r.checksums {
		if rng.stop > latest {
			latest = rng.stop
		}
	}
	return latest + 1, r.err
}

func (r *Repo) FindGaps(start, stop int) ([][2]uint, error) {
	if stop < start {
		return nil, fmt.Errorf("start (%d) is higher than stop (%d) epoch", start, stop)
	}
	var uStart, uStop uint
	if start < 0 {
		uStart = 0
	} else {
		uStart = uint(start)
	}
	if stop < 0 {
		maxRng := r.orderedRanges[len(r.orderedRanges)-1]
		stop = int(maxRng.stop)
	} else {
		uStop = uint(stop)
	}
	// no records, the entire range is a "gap"
	if len(r.orderedRanges) == 0 {
		return [][2]uint{{uStart, uStop}}, r.err
	}
	gaps := make([][2]uint, 0)
	max := len(r.orderedRanges)
	firstRecord := r.orderedRanges[0]
	if firstRecord.start > uStart {
		// the missing head is considered a gap
		gaps = append(gaps, [2]uint{uStart, firstRecord.start - 1})
	}
	for i, current := range r.orderedRanges {
		// haven't entered inspected range yet
		if current.start < uStart {
			continue
		}
		// have exceeded the inspected range, return any collected gaps
		if uStop <= current.stop {
			return gaps, r.err
		}
		nextPosition := i + 1
		// there are no more records, return any collected  gaps
		// the missing tail counts as a gap
		if nextPosition >= max {
			gaps = append(gaps, [2]uint{current.stop + 1, uStop})
			return gaps, r.err
		}
		next := r.orderedRanges[nextPosition]
		// we have a gap between the two records
		if next.start != current.stop+1 {
			gaps = append(gaps, [2]uint{current.stop + 1, next.start - 1})
		}
	}
	return nil, r.err // unreachable
}

func (r *Repo) Interval() uint {
	return r.interval
}

func (r *Repo) Close() error {
	r.checksums = make(map[string]rng)
	r.orderedRanges = make([]rng, 0)
	return r.err
}

func (r *Repo) SetError(err error) {
	r.err = err
}
