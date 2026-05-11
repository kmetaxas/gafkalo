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
	Update UpdateClisterLinkCmd `cmd help:"Update cluster link config"`
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

type UpdateClisterLinkCmd struct {
	Name       string `required help:"Name of the cluster link"`
	ConfigFile string `required help:"Path to cluster link configuration file"`
	Dryrun     bool   `flag help:"If set to 'true' only validation will be performed"`
}

func (cmd *UpdateClisterLinkCmd) Run(ctx *CLIContext) error {
	var linkData ClusterLink
	var diff *ClusterLinkConfigDiff
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
	if cmd.Dryrun {
		needsUpdate, diff, err := admin.NeedsUpdateByLinkName(cmd.Name, &linkData)
		if err != nil {
			return err
		}
		if !needsUpdate {
			fmt.Println("Nothing to do")
		} else {
			fmt.Printf("Would change:\n")
			for key, value := range diff.ChangedConfigs {
				fmt.Printf(" - config '%s' from '%s' to '%s'\n", key, SafeNullStr(value.OldValue), SafeNullStr(value.NewValue))
			}
		}
		return nil
	}

	updated, diff, err := admin.UpdateClusterLink(cmd.Name, &linkData)
	if err != nil {
		return err
	}
	if updated {
		fmt.Print("Updated cluster link\n")
		for key, value := range diff.ChangedConfigs {
			fmt.Printf("Config %s was changed from %s to %s\n", key, SafeNullStr(value.OldValue), SafeNullStr(value.NewValue))
		}

	} else {
		fmt.Println("No changes to cluster link were needed.")
	}

	return nil
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
		fmt.Printf("Unable to list cluster links %s\n", err)
	}
	fmt.Printf("Discovered %d cluster link(s)\n", len(links))
	for name, link := range links {
		fmt.Printf("Link %s: , ClusterID: %s , Configs: %s , Topics: %s\n", name, link.ClusterID, link.Configs, link.MatchedTopics)
	}
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
