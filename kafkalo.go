package main

import (
	"github.com/alecthomas/kong"
)

func main() {
	// Initialize logrus
	ctx := kong.Parse(&CLI)
	err := ctx.Run(&CLIContext{Config: CLI.Config})
	ctx.FatalIfErrorf(err)
}
