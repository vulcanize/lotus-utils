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
	"net/http"
	"os"

	"github.com/filecoin-project/lotus/api/client"
	badgerbs "github.com/filecoin-project/lotus/blockstore/badger"
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

	authTokenFilePath := viper.GetString("repair.auth_token_path")
	if authTokenFilePath == "" {
		logWithCommand.Fatalf("auth token path must be set")
	}
	authToken, err := os.ReadFile(authTokenFilePath)
	if err != nil {
		logWithCommand.Fatalf("unable to read auth token file: %v", err)
	}
	header := http.Header{"Authorization": []string{"Bearer " + string(authToken)}}

	gatewayAPIAddr := viper.GetString("repair.gateway_api_addr")
	if gatewayAPIAddr == "" {
		logWithCommand.Fatalf("gateway API addr must be set")
	}
	gapi, closer, err := client.NewGatewayRPCV1(context.Background(), "http://"+gatewayAPIAddr+"/rpc/v1", header)
	if err != nil {
		logWithCommand.Fatalf("unable to initialize gateway API client: %v", err)
	}
	defer closer()
	repairService := r.NewRepairService(gapi, bs)
	if err := repairService.Repair(context.Background()); err != nil {
		logWithCommand.Fatalf("repair process failed: %v", err)
	}
	logWithCommand.Info("repair process completed successfully")
}

func init() {
	rootCmd.AddCommand(repairCmd)

	repairCmd.PersistentFlags().String("local-blockstore-path", "", "path to local badger blockstore with the missing data we wish to fill in")
	repairCmd.PersistentFlags().String("gateway-api-addr", "", "addr for the Lotus Gateway API to query for the missing data (e.g. 127.0.0.1:1234)")
	repairCmd.PersistentFlags().String("auth-token-path", "", "path to API auth token file")

	viper.BindPFlag("repair.local_blockstore_path", repairCmd.PersistentFlags().Lookup("local-blockstore-path"))
	viper.BindPFlag("repair.gateway_api_addr", repairCmd.PersistentFlags().Lookup("gateway-api-addr"))
	viper.BindPFlag("repair.auth_token_path", repairCmd.PersistentFlags().Lookup("auth-token-path"))
}
