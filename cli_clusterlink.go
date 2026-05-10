package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type CLinkCmd struct {
	List   ListClusterLinksCmd  `cmd help:"List configured cluster links"`
	Create CreateClusterLinkCmd `cmd help:"Create a cluster link"`
	Delete DeleteClusterLinkCmd `cmd help:"Delete a cluster link"`
}
type ListClusterLinksCmd struct {
	Expanded bool `arg default:"false" help:"Expanded status"`
}

type CreateClusterLinkCmd struct {
	Name       string `required help:"Name of the cluster link"`
	ConfigFile string `required help:"Path to cluster link configuration file"`
	Dryrun     bool   `flag help:"If set to 'true' only validation will be performed"`
}

type DeleteClusterLinkCmd struct {
	Name   string `required help:"Name of the cluster link"`
	Dryrun bool   `flag help:"If set to 'true' only validation will be performed"`
	Force  bool   `flag help:"Force delete"`
}

func (cmd *DeleteClusterLinkCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	admin := NewClusterLinkAdmin(config.Connections.RestProxy)
	err := admin.DeleteClusterLink(cmd.Name, cmd.Force, cmd.Dryrun)
	if err != nil {
		return err
	} else {
		if cmd.Dryrun {
			fmt.Printf("Validated deletion of cluster link %s\n", cmd.Name)
		} else {
			fmt.Printf("Deleted cluster link %s\n", cmd.Name)
		}
	}
	return err
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
	var linkData ClusterLink
	config := LoadConfig(ctx.Config)
	admin := NewClusterLinkAdmin(config.Connections.RestProxy)
	// Read config file
	data, err := os.ReadFile(cmd.ConfigFile)
	if err != nil {
		log.Errorf("Failed to read cluster link yaml %s due to %s", cmd.ConfigFile, err)
		return err
	}
	// Unmarshall into a ClusterLink obj
	err = yaml.Unmarshal(data, &linkData)
	if err != nil {
		log.Errorf("Failed to parse cluster link yaml %s due to %s", cmd.ConfigFile, err)
		return err
	}

	err = admin.CreateClusterLink(cmd.Name, &linkData, cmd.Dryrun)
	if cmd.Dryrun {
		if err != nil {
			fmt.Printf("Validation of link %s failed with error %s\n", cmd.Name, err)
		} else {
			fmt.Printf("Dry run validation of link %s passed\n", cmd.Name)
		}
	} else {
		if err != nil {
			fmt.Printf("Failed to create cluster link %s with error: %s\n", cmd.Name, err)
		} else {
			fmt.Printf("Created cluster link %s\n", cmd.Name)
		}
	}

	return nil
}
