package main

import (
	"flag"
	"fmt"
	"log"
)

func main() {
	configFile := flag.String("config", "", "Configuration file location")
	flag.Parse()
	// Make sure we got a configFile passed
	if *configFile == "" {
		log.Fatal("-config must be provided")
	}
	config := parseConfig(*configFile)
	files, err := config.ResolveFilesFromPatterns(config.GetInputPatterns())
	if err != nil {
		log.Fatalf("Failed to get input files: %s\n", err)
	}
	inputData := Parse(files)

	kafkadmin := NewKafkaAdmin()
	//fmt.Printf("Created KafkaAdmin %s\n", kafkadmin)
	_ = kafkadmin.ListTopics()
	topicResults := kafkadmin.ReconcileTopics(inputData.Topics, false, false)
	sradmin := NewSRAdmin(config.Connections.Schemaregistry["url"], config.Connections.Schemaregistry["user"], config.Connections.Schemaregistry["password"])
	schemaResults := sradmin.Reconcile(inputData.Topics, false)
	// Do MDS
	mdsadmin := NewMDSAdmin(config.Connections.Mds)
	//_ = mdsadmin.SetRoleBinding(CTX_KAFKA, "Topic", "SKATARES", "User:arcanum", []string{"ResourceOwner"}, true, false)
	//roles := mdsadmin.getRoleBindingsForPrincipal("User:poutanaola")
	roleResults := mdsadmin.Reconcile(inputData.Clients, false)
	fmt.Printf("Roles for USer = %+vs\n", roleResults)
	NewReport(topicResults, schemaResults, roleResults, false)
}
