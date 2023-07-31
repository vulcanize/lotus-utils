package cmd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/vulcanize/lotus-index-attestation/pkg/attestation"
)

// attestationCmd represents the attestation command
var attestationCmd = &cobra.Command{
	Use:   "attestation",
	Short: "generate msgindex.db checksums and/or expose API for querying persisted checksums",
	Long:  `This command configures a lotus-index-attestation service`,
	Run: func(cmd *cobra.Command, args []string) {
		subCommand = cmd.CalledAs()
		logWithCommand = *log.WithField("SubCommand", subCommand)
		attestationService()
	},
}

func attestationService() {
	attestationConfig, err := attestation.NewConfig()
	if err != nil {
		logWithCommand.Fatal(err)
	}
	logWithCommand.Debug("attestation config: %+v", attestationConfig)
	service, err := attestation.NewServiceFromConfig(attestationConfig)
	if err != nil {
		logWithCommand.Fatal(err)
	}
	wg := new(sync.WaitGroup)
	ctx := context.Background()
	errChan := make(<-chan error)
	if attestationConfig.Checksum {
		logWithCommand.Info("beginning attestation checksumming process")
		err, errChan = service.Checksum(ctx, wg)
		if err != nil {
			logWithCommand.Fatal(err)
		}
	}
	if attestationConfig.Serve {
		logWithCommand.Info("beginning attestation server")
		if err := service.Register(rpc.Register); err != nil {
			logWithCommand.Fatal(err)
		}
		rpc.HandleHTTP()
		listener, err := net.Listen("tcp", fmt.Sprintf(":%s", attestationConfig.ServerPort))
		if err != nil {
			log.Fatal("listen error:", err)
		}
		go http.Serve(listener, nil)
		if err := service.Serve(ctx, wg); err != nil {
			logWithCommand.Fatal(err)
		}
	}

	go func() {
		for {
			select {
			case err := <-errChan:
				// TODO: add additional error handling logic e.g. shutdown on error
				logWithCommand.Error(err)
			}
		}
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt)
	<-shutdown
	if err := service.Close(); err != nil {
		logWithCommand.Fatal(err)
	}
	wg.Wait()
}

func init() {
	rootCmd.AddCommand(attestationCmd)
}
