package repair

import (
	"context"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/blockstore"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/sirupsen/logrus"
)

type Service struct {
	srcAPI api.Gateway
	dstBS  blockstore.Blockstore
}

func NewRepairService(srcAPI api.Gateway, dstBS blockstore.Blockstore) *Service {
	return &Service{
		srcAPI: srcAPI,
		dstBS:  dstBS,
	}
}

func (rs *Service) Repair(ctx context.Context, missingCIDs []cid.Cid) error {
	logrus.Infof("retrieving missing blocks for CIDs: %+v", missingCIDs)
	blocks, err := rs.retrieveMissingBlocks(ctx, missingCIDs)
	if err != nil {
		return err
	}
	logrus.Infof("inserting missing blocks for CIDs: %+v", missingCIDs)
	if err := rs.dstBS.PutMany(ctx, blocks); err != nil {
		return err
	}
	return nil
}

func (rs *Service) retrieveMissingBlocks(ctx context.Context, missingCIDs []cid.Cid) ([]block.Block, error) {
	blocks := make([]block.Block, len(missingCIDs))
	for i, c := range missingCIDs {
		b, err := rs.srcAPI.ChainReadObj(ctx, c)
		if err != nil {
			return nil, err
		}
		blk, err := block.NewBlockWithCid(b, c)
		if err != nil {
			return nil, err
		}
		blocks[i] = blk
	}
	return blocks, nil
}
