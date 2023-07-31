package mocks

import "github.com/vulcanize/lotus-utils/pkg/types"

var _ types.Checksummer = &CheckSummer{}

type CheckSummer struct {
	gaps     [][2]uint
	checkSum string
	err      error
}

func (c *CheckSummer) FindGaps(start, stop int) ([][2]uint, error) {
	return c.gaps, c.err
}

func (c *CheckSummer) SetGaps(gaps [][2]uint) {
	c.gaps = gaps
}

func (c *CheckSummer) Checksum(start, stop uint) (string, error) {
	return c.checkSum, c.err
}

func (c *CheckSummer) SetChecksum(hash string) {
	c.checkSum = hash
}

func (c *CheckSummer) SetErr(err error) {
	c.err = err
}

func (c *CheckSummer) CheckRangeIsPopulated(start, stop uint) (bool, error) {
	return len(c.gaps) == 0, c.err
}

func (c *CheckSummer) Close() error {
	c.gaps = make([][2]uint, 0)
	return c.err
}
