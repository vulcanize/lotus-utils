package repair

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/blockstore"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
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

func (rs *Service) Repair(ctx context.Context) error {
	missingCIDs, err := getMissingCIDs(ctx)
	if err != nil {
		return err
	}
	blocks, err := rs.retrieveMissingBlocks(ctx, missingCIDs)
	if err != nil {
		return err
	}
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

func getMissingCIDs(ctx context.Context) ([]cid.Cid, error) {
	// prompt the use for either a filepath or for a comma separated list of CIDs
	var inputType string
	var file bool
	fmt.Printf("Do you want to provide a file path to a file containing the missing CIDs or a comma separated list of CIDs? (file/list; f/l): ")
	_, err := fmt.Scanln(&inputType)
	if err != nil {
		return nil, err
	}
	if strings.ToLower(inputType) == "file" || strings.ToLower(inputType) == "f" {
		file = true
	} else if strings.ToLower(inputType) == "list" || strings.ToLower(inputType) == "l" {
		file = false
	} else {
		return nil, fmt.Errorf("invalid input type: %s", inputType)
	}
	var cids []cid.Cid
	if file {
		var filePath string
		fmt.Printf("Enter the path to a file containing the missing CIDs: ")
		_, err := fmt.Scanln(&filePath)
		if err != nil {
			return nil, err
		}
		file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
		if err != nil {
			return nil, err
		}
		cids, err = ParseMissingCIDs(file)
		if err != nil {
			return nil, err
		}
	} else {
		var cidListStr string
		fmt.Printf("Enter a comma separated list of CIDs: ")
		_, err := fmt.Scanln(&cidListStr)
		if err != nil {
			return nil, err
		}
		strippedList := strings.ReplaceAll(cidListStr, " ", "")
		cidList := strings.Split(strippedList, ",")
		cids = make([]cid.Cid, len(cidList))
		for i, cidStr := range cidList {
			c, err := cid.Decode(cidStr)
			if err != nil {
				return nil, err
			}
			cids[i] = c
		}
	}
	return cids, nil
}
