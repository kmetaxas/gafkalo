package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type CLinkCmd struct {
	List   ListClusterLinksCmd  `cmd help:"List configured cluster links"`
	Create CreateClusterLinkCmd `cmd help:"Create a cluster link"`
}
type ListClusterLinksCmd struct {
	Expanded bool `default:"false" help:"Expanded status"`
}

type CreateClusterLinkCmd struct {
	Name       string `help:"Name of the cluster link"`
	ConfigFile string `help:"Path to cluster link configuration file"`
	Dryrun     bool   `default:"false" help:"If set to 'true' only validation will be performed"`
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

func (cmd *CreateClusterLinkCmd) Run(ctx *CLIContext) error {
	var linkData *ClusterLink
	config := LoadConfig(ctx.Config)
	admin := NewClusterLinkAdmin(config.Connections.RestProxy)
	// Read config file
	data, err := os.ReadFile(cmd.ConfigFile)
	if err != nil {
		return err
	}
	// Unmarshall into a ClusterLink obj
	err = json.Unmarshal(data, linkData)
	if err != nil {
		return err
	}

	admin.CreateClusterLink(cmd.Name, linkData, cmd.Dryrun)
	return nil
}
