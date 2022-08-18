package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"text/template"
)
import _ "embed"

//go:embed templates/topic_describe.tpl
var topicDescribeTmplData string

type TopicCmd struct {
	Describe DescribeTopicCmd `cmd help:"Describe topic"`
	List     ListTopicsCmd    `cmd help:"List topics"`
}

type DescribeTopicCmd struct {
	Name string `arg required help:"Topic name"`
}

type ListTopicsCmd struct {
	Describe bool `help:"Describe topics as well" `
}

func (cmd *ListTopicsCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	kafkadmin := NewKafkaAdmin(config.Connections.Kafka)
	topics := kafkadmin.ListTopics()
	for name, topic := range topics {
		if cmd.Describe {
			fmt.Printf("Topic:%s\tPartitions:%d\tReplicationFactor:%d\n", name, topic.Partitions,
				topic.ReplicationFactor)
		} else {
			fmt.Printf("%s\n", name)
		}
	}
	return nil
}

func (cmd *DescribeTopicCmd) Run(ctx *CLIContext) error {
	type TopicDescribeContext struct {
		Name    string
		Details interface{}
	}
	config := LoadConfig(ctx.Config)
	kafkadmin := NewKafkaAdmin(config.Connections.Kafka)
	topics := kafkadmin.ListTopics()
	if topicDetails, ok := topics[cmd.Name]; ok {
		context := TopicDescribeContext{Name: cmd.Name, Details: topicDetails}
		tmpl := template.Must(template.New("console").Parse(topicDescribeTmplData))
		err := tmpl.Execute(os.Stdout, context)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		return fmt.Errorf("no such topic: %s", cmd.Name)
	}
	return nil
}
