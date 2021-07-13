package main

import (
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"log"
	"os"
)

type ConnectCmd struct {
	List     ListConnectorsCmd    `cmd help:"List configured connectors"`
	Describe DescribeConnectorCmd `cmd help:"Describe connector"`
}

type ListConnectorsCmd struct {
}
type DescribeConnectorCmd struct {
	Connector string `cmd required help:"Connector name"`
}

// Describe a connector.
func (cmd *DescribeConnectorCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	admin, err := NewConnectAdin(&config.Connections.Connect)
	if err != nil {
		log.Fatal(err)
	}
	connectorInfo, _ := admin.GetConnectorInfo(cmd.Connector)
	fmt.Printf("Connector Info\n")
	fmt.Printf("Name: %s\n", connectorInfo.Name)
	fmt.Println("Configs:")
	for key, value := range connectorInfo.Config {
		fmt.Printf("   %s = %s\n", key, value)
	}
	fmt.Printf("Tasks: %d\n", len(connectorInfo.Tasks))
	tasks, err := admin.ListTasksForConnector(cmd.Connector)
	if err != nil {
		log.Fatal(err)
	}

	for _, task := range tasks {
		prettyPrintTaskStatus(task)

	}
	return nil
}

func (cmd *ListConnectorsCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	admin, err := NewConnectAdin(&config.Connections.Connect)
	if err != nil {
		log.Fatal(err)
	}
	connectors, err := admin.ListConnectors()
	if err != nil {
		log.Fatal(err)
	}
	tb := table.NewWriter()
	tb.SetStyle(table.StyleLight)
	tb.SetOutputMirror(os.Stdout)
	tb.AppendHeader(table.Row{"#", "Connector name"})
	for i, name := range connectors {
		tb.AppendRow(table.Row{i, name})
	}
	tb.Render()

	return nil
}
