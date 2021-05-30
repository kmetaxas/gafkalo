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
	sradmin := NewSRAdmin("http://localhost:8081", config.Connections.Schemaregistry["user"], config.Connections.Schemaregistry["password"])
	//fmt.Printf("SubjectCache = %s\n", sradmin.SubjectCache)
	for _, topic := range inputData.Topics {
		if (Schema{} != topic.Value) {
			sradmin.LookupSchema(topic.Value)
			res := sradmin.Reconcile(topic.Value, false)
			fmt.Printf("Reconcile result=%+vs\n", res)
			//sradmin.RegisterSubject(topic.Value)
			//curCompat, _ := sradmin.GetCompatibility(topic.Value)
			//log.Printf("Compat for topic %s = %s", topic.Name, curCompat)
			//sradmin.SetCompatibility(topic.Value, "FORWARD")
		}
		if (Schema{} != topic.Key) {
			sradmin.LookupSchema(topic.Key)
			sradmin.RegisterSubject(topic.Key)
		}

	}
	NewReport(topicResults)
}
