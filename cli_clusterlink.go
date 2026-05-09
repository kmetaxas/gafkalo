package main

import "fmt"

type CLinkCmd struct {
	List ListClusterLinksCmd `cmd help:"List configured cluster links"`
}
type ListClusterLinksCmd struct {
	Expanded bool `default:"false" help:"Expanded status"`
}

func (cmd *ListClusterLinksCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	admin := NewClusterLinkAdmin(config.Connections.RestProxy)
	links, err := admin.ListClusterLinks()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Links = %+v", links)
	return nil
}
