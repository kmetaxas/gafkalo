package main

import "fmt"

type SrvCmd struct {
	Serve SrvServeCmd `cmd: help:"Start serving"`
}

type SrvServeCmd struct {
	// Add any parameters for this command. currently none
}

func (cmd *SrvServeCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	srv := NewAPIServer(config)
	port := config.Kafkalo.RestConfig.Port

	err := srv.Start(fmt.Sprintf(":%d", port))
	return err
}
