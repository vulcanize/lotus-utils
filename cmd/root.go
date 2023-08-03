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
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/vulcanize/lotus-utils/pkg/attestation"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile        string
	envFile        string
	subCommand     string
	logWithCommand log.Entry
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:              "lotus-utils",
	PersistentPreRun: initFuncs,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	log.Info("----- Starting Lotus Utils -----")
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func initFuncs(cmd *cobra.Command, args []string) {
	logInit()

	// TODO: add metrics
	/*
		if viper.GetBool("metrics") {
			prom.Init()
		}

		if viper.GetBool("prom.http") {
			addr := fmt.Sprintf(
				"%s:%s",
				viper.GetString("prom.http.addr"),
				viper.GetString("prom.http.port"),
			)
			prom.Serve(addr)
		}
	*/
}

func init() {
	cobra.OnInitialize(initConfig)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.lotus-utils.yaml)")
	rootCmd.PersistentFlags().StringVar(&envFile, "env", "", "environment file location")

	rootCmd.PersistentFlags().String("log-level", log.InfoLevel.String(), "log level (trace, debug, info, warn, error, fatal, panic)")
	rootCmd.PersistentFlags().String("log-file", "", "file path for logging")

	viper.BindPFlag(attestation.LOG_LEVEL_TOML, rootCmd.PersistentFlags().Lookup("log-level"))
	viper.BindPFlag(attestation.LOG_FILE_TOML, rootCmd.PersistentFlags().Lookup("log-file"))

	// TODO: add metrics
	/*
		rootCmd.PersistentFlags().Bool("metrics", false, "enable metrics")

		rootCmd.PersistentFlags().Bool("prom-http", false, "enable http service for prometheus")
		rootCmd.PersistentFlags().String("prom-http-addr", "127.0.0.1", "http host for prometheus")
		rootCmd.PersistentFlags().String("prom-http-port", "8090", "http port for prometheus")

		viper.BindPFlag("metrics", rootCmd.PersistentFlags().Lookup("metrics"))

		viper.BindPFlag("prom.http", rootCmd.PersistentFlags().Lookup("prom-http"))
		viper.BindPFlag("prom.http.addr", rootCmd.PersistentFlags().Lookup("prom-http-addr"))
		viper.BindPFlag("prom.http.port", rootCmd.PersistentFlags().Lookup("prom-http-port"))
	*/
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".lotus-utils" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".lotus-utils")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}

func logInit() error {
	// Set the output.
	viper.BindEnv(attestation.LOG_FILE_TOML, attestation.LOG_FILE)
	logFile := viper.GetString("log.file")
	if logFile != "" {
		file, err := os.OpenFile(logFile,
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
		if err == nil {
			log.Infof("Directing output to %s", logFile)
			log.SetOutput(file)
		} else {
			log.SetOutput(os.Stdout)
			log.Info("Failed to logrus.to file, using default stdout")
		}
	} else {
		log.SetOutput(os.Stdout)
	}

	// Set the level.
	viper.BindEnv(attestation.LOG_LEVEL_TOML, attestation.LOG_LEVEL)
	lvl, err := log.ParseLevel(viper.GetString("log.level"))
	if err != nil {
		return err
	}
	log.SetLevel(lvl)

	formatter := &log.TextFormatter{
		FullTimestamp: true,
	}
	// Show file/line number only at Trace level.
	if lvl >= log.TraceLevel {
		log.SetReportCaller(true)

		// We need to exclude this wrapper code, logrus.us itself, and the runtime from the stack to show anything useful.
		// cf. https://github.com/sirupsen/logrus.us/pull/973
		formatter.CallerPrettyfier = func(frame *runtime.Frame) (function string, file string) {
			pcs := make([]uintptr, 50)
			_ = runtime.Callers(0, pcs)
			frames := runtime.CallersFrames(pcs)

			// Filter logrus.wrapper / logrus.us / runtime frames.
			for next, again := frames.Next(); again; next, again = frames.Next() {
				if !strings.Contains(next.File, "sirupsen/logrus.us") &&
					!strings.HasPrefix(next.Function, "runtime.") &&
					!strings.Contains(next.File, "lotus-index-attestation/pkg/log") {
					return next.Function, fmt.Sprintf("%s:%d", next.File, next.Line)
				}
			}

			// Fallback to the raw info.
			return frame.Function, fmt.Sprintf("%s:%d", frame.File, frame.Line)
		}
	}

	log.SetFormatter(formatter)
	log.Info("Log level set to ", lvl.String())
	return nil
}
