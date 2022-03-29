package main

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
)

type ConnectCmd struct {
	List        ListConnectorsCmd    `cmd help:"List configured connectors"`
	Describe    DescribeConnectorCmd `cmd help:"Describe connector"`
	Create      CreateConnectorCmd   `cmd help:"Create connector"`
	Delete      DeleteConnectorCmd   `cmd help:"Delete connector"`
	Heal        HealCmd              `cmd help:"Heal connector by restarting any failed tasks"`
	HealthCheck HealthCheckCmd       `cmd help:"Health Check on connector(s)"`
}

type ListConnectorsCmd struct {
}

type HealthCheckCmd struct {
}
type DescribeConnectorCmd struct {
	Name string `arg required help:"Connector name"`
}
type CreateConnectorCmd struct {
	JsonFile string `arg required help:"path to JSON definition for connector"`
}

type DeleteConnectorCmd struct {
	Name string `arg  help:"Connector name"`
}

type HealCmd struct {
	Name string `arg  help:"Connector name"`
}

// Describe a connector.
func (cmd *DescribeConnectorCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	admin, err := NewConnectAdin(&config.Connections.Connect)
	if err != nil {
		log.Fatal(err)
	}
	connectorInfo, _ := admin.GetConnectorInfo(cmd.Name)
	fmt.Printf("Connector: %s\n", connectorInfo.Name)
	status, err := admin.GetConnectorStatus(cmd.Name)
	if err != nil {
		return err
	}
	statusTb := table.NewWriter()
	statusTb.SetStyle(table.StyleLight)
	statusTb.SetOutputMirror(os.Stdout)
	statusTb.AppendHeader(table.Row{"Key", "Value"})
	for key, value := range status.Connector {
		statusTb.AppendRow(table.Row{key, value})
	}
	statusTb.Render()

	tb := table.NewWriter()
	tb.SetStyle(table.StyleLight)
	tb.SetOutputMirror(os.Stdout)
	tb.AppendHeader(table.Row{"Config name", "Config value"})
	for key, value := range connectorInfo.Config {
		tb.AppendRow(table.Row{key, value})
	}
	tb.Render()
	tasks, err := admin.ListTasksForConnector(cmd.Name)
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
	name, err := admin.CreateConnector(string(data))
	if err != nil {
		return err
	}
	fmt.Printf("Created conector%s\n", name)
	return nil
}

func (cmd *DeleteConnectorCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	admin, err := NewConnectAdin(&config.Connections.Connect)
	if err != nil {
		return err
	}
	err = admin.DeleteConnector(cmd.Name)
	if err != nil {
		return err
	}
	fmt.Printf("Deleted connector %s\n", cmd.Name)
	return nil
}

func (cmd *HealthCheckCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	admin, err := NewConnectAdin(&config.Connections.Connect)
	var faultyConnectors int = 0
	healthyColor := color.New(color.FgGreen).SprintFunc()
	errorColor := color.New(color.FgRed).SprintFunc()
	if err != nil {
		return err
	}
	connectors, err := admin.ListConnectors()
	if err != nil {
		return err
	}
	for _, connectorName := range connectors {
		status, err := admin.GetConnectorStatus(connectorName)
		if err != nil {
			return err
		}
		if !status.isHealthy() {
			fmt.Fprintf(os.Stderr, "Connector %s is not healthy\n", errorColor(connectorName))
			faultyConnectors += 1
		}
	}
	if faultyConnectors == 0 {
		fmt.Printf("All connectors are %s (%d connectors checked)\n", healthyColor("healthy"), len(connectors))
	} else {
		fmt.Fprintf(os.Stderr, "%s connectors (out of %d total)\n", errorColor(faultyConnectors, " ERROR"), len(connectors))
	}
	return nil
}

func (cmd *HealCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	admin, err := NewConnectAdin(&config.Connections.Connect)
	if err != nil {
		return err
	}
	status, err := admin.GetConnectorStatus(cmd.Name)
	if err != nil {
		return err
	}
	if status.isHealthy() {
		fmt.Printf("Connector %s is healthy. Doing nothing.\n", cmd.Name)
		return nil
	}
	// Not healthy so lets try restarting stuff
	// First the connector itself
	fmt.Printf("Restarting connector %s\n", cmd.Name)
	err = admin.RestartConnector(cmd.Name)
	if err != nil {
		return err
	}
	// Now the tasks one by one
	tasks, err := admin.ListTasksForConnector(cmd.Name)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if task.Status != "RUNNING" {
			fmt.Printf("Restarting task %d..\n", task.ID)
			admin.RestartTask(cmd.Name, task.ID)
		}
	}
	return nil
}
