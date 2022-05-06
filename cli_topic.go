package main

import (
	"fmt"
)

type TopicCmd struct {
	Describe DescribeTopicCmd `cmd help:"Describe topic"`
}

type DescribeTopicCmd struct {
	Name string `arg required help:"Topic name"`
}

func (cmd *DescribeTopicCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	kafkadmin, _, _ := GetAdminClients(config)
	topics := kafkadmin.ListTopics()
	if topicDetails, ok := topics[cmd.Name]; ok {
		// display partitions and replication factor
		fmt.Printf("Partitions: %d, ReplicationFactor: %d\n", topicDetails.NumPartitions, topicDetails.ReplicationFactor)
		// display configs
		fmt.Printf("Configs: ")
		for name, value := range topicDetails.ConfigEntries {
			fmt.Printf("%s=%s ", name, *value)
		}
		fmt.Printf("\n")
		// display replica assignments
		// TODO replace this look+conditional crap with a Go template .
		for partition, assignments := range topicDetails.ReplicaAssignment {
			fmt.Printf("Partition %d assignments: ", partition)
			for index, broker_id := range assignments {
				isLeader := false
				if index == 0 {
					isLeader = true
				}
				fmt.Printf("%d ", broker_id)
				if isLeader {
					fmt.Printf("[Leader] ")
				}
			}
		}
	} else {
		return fmt.Errorf("No such topic: %s", cmd.Name)
	}
	return nil
}
