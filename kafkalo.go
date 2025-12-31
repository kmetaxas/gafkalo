package main

import (
	"fmt"
	"os"

	"github.com/IBM/sarama"
	"github.com/alecthomas/kong"
	kongcompletion "github.com/jotaen/kong-completion"
	log "github.com/sirupsen/logrus"
)

func main() {
	var logLevel log.Level
	var err error
	// Initialize kong
	CLI := CliApp{}
	kongApp := kong.Must(&CLI)
	kongcompletion.Register(kongApp)

	ctx, err := kongApp.Parse(os.Args[1:])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

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
