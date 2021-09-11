package main

import (
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"io/ioutil"
	"log"
	"os"
)

type ConnectCmd struct {
	List     ListConnectorsCmd    `cmd help:"List configured connectors"`
	Describe DescribeConnectorCmd `cmd help:"Describe connector"`
	Create   CreateConnectorCmd   `cmd help:"Create connector"`
}

type ListConnectorsCmd struct {
}
type DescribeConnectorCmd struct {
	Connector string `cmd required help:"Connector name"`
}
type CreateConnectorCmd struct {
	Name     string `cmd required help:"Connector name"`
	JsonFile string `required help:"path to JSON definition for connector"`
}

// Describe a connector.
func (cmd *DescribeConnectorCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	admin, err := NewConnectAdin(&config.Connections.Connect)
	if err != nil {
		log.Fatal(err)
	}
	connectorInfo, _ := admin.GetConnectorInfo(cmd.Connector)
	fmt.Printf("Connector: %s\n", connectorInfo.Name)
	tb := table.NewWriter()
	tb.SetStyle(table.StyleLight)
	tb.SetOutputMirror(os.Stdout)
	tb.AppendHeader(table.Row{"Config name", "Config value"})
	for key, value := range connectorInfo.Config {
		tb.AppendRow(table.Row{key, value})
	}
	tb.Render()
	tasks, err := admin.ListTasksForConnector(cmd.Connector)
	if err != nil {
		log.Fatal(err)
	}
	tasktb := table.NewWriter()
	tasktb.SetStyle(table.StyleLight)
	tasktb.SetOutputMirror(os.Stdout)
	tasktb.AppendHeader(table.Row{"ID", "STATUS", "WORKER", "Is running"})
	for _, task := range tasks {
		tasktb.AppendRow(table.Row{task.ID, task.Status, task.WorkerID, task.isRunning})

	}
	tasktb.Render()
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

func (cmd *CreateConnectorCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	admin, err := NewConnectAdin(&config.Connections.Connect)
	if err != nil {
		log.Fatal(err)
	}
	data, err := ioutil.ReadFile(cmd.JsonFile)
	if err != nil {
		log.Printf("unable to read %s with error %s\n", cmd.JsonFile, err)
	}
	err = admin.CreateConnector(cmd.Name, string(data))
	if err != nil {
		return err
	}
	return nil
}
