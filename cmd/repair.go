/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/filecoin-project/lotus/api/client"
	badgerbs "github.com/filecoin-project/lotus/blockstore/badger"
	"github.com/ipfs/go-cid"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	r "github.com/vulcanize/lotus-utils/pkg/repair"
)

// repairCmd represents the repair command
var repairCmd = &cobra.Command{
	Use:   "repair",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		subCommand = cmd.CalledAs()
		logWithCommand = *log.WithField("SubCommand", subCommand)
		repair()
	},
}

func repair() {
	localBlockStorePath := viper.GetString("repair.local_blockstore_path")
	if localBlockStorePath == "" {
		logWithCommand.Fatalf("local blockstore path must be set")
	}
	bs, err := badgerbs.Open(badgerbs.DefaultOptions(localBlockStorePath))
	if err != nil {
		logWithCommand.Fatalf("unable to open local blockstore: %v", err)
	}

	header := http.Header{}
	header.Add("Content-Type", "application/javascript")
	authTokenFilePath := viper.GetString("repair.auth_token_path")
	if authTokenFilePath == "" {
		logWithCommand.Warn("auth token path not set")
	} else {
		authToken, err := os.ReadFile(authTokenFilePath)
		if err != nil {
			logWithCommand.Fatalf("unable to read auth token file: %v", err)
		}
		header.Add("Authorization", "Bearer "+string(authToken))
	}
	gatewayAPIURL := viper.GetString("repair.gateway_api_url")
	if gatewayAPIURL == "" {
		logWithCommand.Fatalf("gateway api url must be set")
	}
	gapi, closer, err := client.NewGatewayRPCV1(context.Background(), gatewayAPIURL, header)
	if err != nil {
		logWithCommand.Fatalf("unable to initialize gateway API client: %v", err)
	}
	defer closer()
	missingCIDs, err := getMissingCIDs()
	if err != nil {
		logWithCommand.Fatalf("unable to get missing CIDs: %v", err)
	}
	repairService := r.NewRepairService(gapi, bs)
	if err := repairService.Repair(context.Background(), missingCIDs); err != nil {
		logWithCommand.Fatalf("repair process failed: %v", err)
	}
	logWithCommand.Info("repair process completed successfully")
}

func getMissingCIDs() ([]cid.Cid, error) {
	errorFilePath := viper.GetString("repair.error_file_path")
	missingCIDStrs := viper.GetStringSlice("repair.missing_cids")
	if len(missingCIDStrs) == 0 && errorFilePath == "" {
		return nil, fmt.Errorf("need to specifiy either a file path or a list of missing CIDs")
	}
	missingCids := make([]cid.Cid, 0)
	for _, cidStr := range missingCIDStrs {
		c, err := cid.Decode(cidStr)
		if err != nil {
			return nil, err
		}
		missingCids = append(missingCids, c)
	}
	if errorFilePath != "" {
		file, err := os.OpenFile(errorFilePath, os.O_RDONLY, 0666)
		if err != nil {
			return nil, err
		}
		cids, err := r.ParseMissingCIDs(file)
		if err != nil {
			return nil, err
		}
		missingCids = combineCIDs(missingCids, cids)
	}
	return missingCids, nil
}

func combineCIDs(cids1 []cid.Cid, cids2 []cid.Cid) []cid.Cid {
	cidMap := map[cid.Cid]struct{}{}
	for _, c := range cids1 {
		cidMap[c] = struct{}{}
	}
	for _, c := range cids2 {
		cidMap[c] = struct{}{}
	}
	returnCIDs := make([]cid.Cid, 0, len(cidMap))
	for c := range cidMap {
		returnCIDs = append(returnCIDs, c)
	}
	return returnCIDs
}

func init() {
	rootCmd.AddCommand(repairCmd)

	repairCmd.PersistentFlags().String("local-blockstore-path", "", "path to local badger blockstore with the missing data we wish to fill in")
	repairCmd.PersistentFlags().String("gateway-api-url", "", "URL for the Lotus Gateway API to query for the missing data (e.g. 127.0.0.1:1234)")
	repairCmd.PersistentFlags().String("auth-token-path", "", "path to API auth token file")
	repairCmd.PersistentFlags().String("error-file-path", "", "path to file with the error logs from which to extract the missing CIDs")
	repairCmd.PersistentFlags().StringArray("missing-cids", []string{}, "comma separated list of CIDs that are missing the blockstore")

	viper.BindPFlag("repair.local_blockstore_path", repairCmd.PersistentFlags().Lookup("local-blockstore-path"))
	viper.BindPFlag("repair.gateway_api_url", repairCmd.PersistentFlags().Lookup("gateway-api-url"))
	viper.BindPFlag("repair.auth_token_path", repairCmd.PersistentFlags().Lookup("auth-token-path"))
	viper.BindPFlag("repair.error_file_path", repairCmd.PersistentFlags().Lookup("error-file-path"))
	viper.BindPFlag("repair.missing_cids", repairCmd.PersistentFlags().Lookup("missing-cids"))
}
