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
		fmt.Printf("encountered Oos %s\n", err)
	}
	fmt.Printf("Links = %+v\n", links)
	return nil
}
