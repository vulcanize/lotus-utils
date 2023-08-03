package repair

import (
	"bufio"
	"io"
	"strings"

	"github.com/ipfs/go-cid"
)

var v1DAGCBORCidPrefix = "bafy2bzace"
var v1DAGCBORCidLength = 62

func ParseMissingCIDs(src io.ReadCloser) ([]cid.Cid, error) {
	scanner := bufio.NewScanner(src)
	defer src.Close()
	var missingCIDs []cid.Cid
	for scanner.Scan() {
		line := scanner.Text()
		cids, err := parseLine(line)
		if err != nil {
			return nil, err
		}
		missingCIDs = append(missingCIDs, cids...)
	}
	return dedupeCIDs(missingCIDs), nil
}

func dedupeCIDs(cids []cid.Cid) []cid.Cid {
	cidMap := make(map[cid.Cid]struct{})
	for _, c := range cids {
		cidMap[c] = struct{}{}
	}
	returnCIDs := make([]cid.Cid, 0, len(cidMap))
	for c := range cidMap {
		returnCIDs = append(returnCIDs, c)
	}
	return returnCIDs
}

func parseLine(line string) ([]cid.Cid, error) {
	cids := make([]cid.Cid, 0)
	for {
		c, remainder, ok, err := extractNextCID(line)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		cids = append(cids, c)
		line = remainder
	}
	return cids, nil
}

func extractNextCID(str string) (cid.Cid, string, bool, error) {
	if strings.Contains(str, v1DAGCBORCidPrefix) {
		index := strings.Index(str, v1DAGCBORCidPrefix)
		cidStr := str[index : index+v1DAGCBORCidLength]
		c, err := cid.Decode(cidStr)
		if err != nil {
			return cid.Cid{}, "", false, err
		}
		return c, str[index+v1DAGCBORCidLength:], true, nil
	}
	return cid.Cid{}, "", false, nil
}
