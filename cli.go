package main

import (
	"fmt"
	"log"
)

type CLIContext struct {
	Config string `required arg help:"configuration file"`
}
type PlanCmd struct {
	Dryrun bool `default:"true" hidden`
}

type ApplyCmd struct {
	Dryrun bool `default:"false" hidden`
}

var CLI struct {
	Config string   `required help:"configuration file"`
	Apply  ApplyCmd `cmd help:"Apply the changes"`
	Plan   PlanCmd  `cmd help:"Produce a plan of changes"`
}

func (cmd *ApplyCmd) Run(ctx *CLIContext) error {
	fmt.Printf("Running ApplyCmd%+vs\n", cmd)
	config := LoadConfig(ctx.Config)
	inputData := GetInputData(config)
	kafkadmin, sradmin, mdsadmin := GetAdminClients(config)
	DoSync(&kafkadmin, &sradmin, &mdsadmin, &inputData, true)
	return nil
}
func (cmd *PlanCmd) Run(ctx *CLIContext) error {
	fmt.Printf("Running PlanCmd %+vs\n", cmd)
	config := LoadConfig(ctx.Config)
	inputData := GetInputData(config)
	kafkadmin, sradmin, mdsadmin := GetAdminClients(config)
	DoSync(&kafkadmin, &sradmin, &mdsadmin, &inputData, false)
	return nil
}
func LoadConfig(config string) Configuration {
	configuration := parseConfig(config)
	return configuration
}

func GetInputData(config Configuration) DesiredState {
	files, err := config.ResolveFilesFromPatterns(config.GetInputPatterns())
	if err != nil {
		log.Fatalf("Failed to get input files: %s\n", err)
	}
	inputData := Parse(files)
	return inputData
}

func GetAdminClients(config Configuration) (KafkaAdmin, SRAdmin, MDSAdmin) {
	kafkadmin := NewKafkaAdmin()
	sradmin := NewSRAdmin(config.Connections.Schemaregistry["url"], config.Connections.Schemaregistry["user"], config.Connections.Schemaregistry["password"])
	mdsadmin := NewMDSAdmin(config.Connections.Mds)
	return kafkadmin, sradmin, *mdsadmin
}
func DoSync(kafkadmin *KafkaAdmin, sradmin *SRAdmin, mdsadmin *MDSAdmin, inputData *DesiredState, dryRun bool) {
	topicResults := kafkadmin.ReconcileTopics(inputData.Topics, false, false)
	schemaResults := sradmin.Reconcile(inputData.Topics, false)
	// Do MDS
	//_ = mdsadmin.SetRoleBinding(CTX_KAFKA, "Topic", "SKATARES", "User:arcanum", []string{"ResourceOwner"}, true, false)
	//roles := mdsadmin.getRoleBindingsForPrincipal("User:poutanaola")
	roleResults := mdsadmin.Reconcile(inputData.Clients, false)
	fmt.Printf("Roles for USer = %+vs\n", roleResults)
	NewReport(topicResults, schemaResults, roleResults, false)
}
