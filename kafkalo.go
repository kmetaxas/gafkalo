package main

import (
	"github.com/IBM/sarama"
	"github.com/alecthomas/kong"
	log "github.com/sirupsen/logrus"
)

func main() {
	var logLevel log.Level
	var err error
	// Initialize kong
	ctx := kong.Parse(&CLI)

	// Initialize logrus
	log.SetReportCaller(true)
	logLevel, err = log.ParseLevel(CLI.Verbosity)
	if err != nil {
		log.Printf("Could not parse log level. Setting to 'error'")
		logLevel = log.ErrorLevel
	}
	log.SetLevel(logLevel)
	// Set sarama logging
	sarama_logger := log.New()
	sarama_logger.SetReportCaller(true)
	sarama_logger.SetLevel(logLevel)
	sarama.Logger = sarama_logger
	if CLI.Verbosity == "trace" || CLI.Verbosity == "debug" {
		sarama.DebugLogger = sarama_logger
	}
	// Run Kong CLI command user chose
	err = ctx.Run(&CLIContext{Config: CLI.Config})
	ctx.FatalIfErrorf(err)
}
