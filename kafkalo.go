package main

import (
	"github.com/Shopify/sarama"
	"github.com/alecthomas/kong"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func main() {
	var logLevel logrus.Level
	var err error
	// Initialize kong
	ctx := kong.Parse(&CLI)

	// Initialize logrus
	log.SetReportCaller(true)
	logLevel, err = logrus.ParseLevel(CLI.Verbosity)
	if err != nil {
		log.Printf("Could not parse log level. Setting to 'error'")
		logLevel = logrus.ErrorLevel
	}
	log.SetLevel(logLevel)
	//Set sarama logging
	log.SetReportCaller(true)
	log.SetLevel(logLevel)

	sarama.Logger = log
	log.SetLevel(logLevel)
	if CLI.Verbosity == "trace" || CLI.Verbosity == "debug" {
		sarama.DebugLogger = log
	}
	// Run Kong CLI command user chose
	err = ctx.Run(&CLIContext{Config: CLI.Config})
	ctx.FatalIfErrorf(err)
}
