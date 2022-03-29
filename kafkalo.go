package main

import (
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
	// Run Kong CLI command user chose
	err = ctx.Run(&CLIContext{Config: CLI.Config})
	ctx.FatalIfErrorf(err)
}
