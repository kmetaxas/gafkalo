package main

import (
	"fmt"
	"os"
	"text/template"

	log "github.com/sirupsen/logrus"

	_ "embed"
)

//go:embed templates/topic_describe.tpl
var topicDescribeTmplData string

type TopicCmd struct {
	Describe DescribeTopicCmd `cmd help:"Describe topic"`
	List     ListTopicsCmd    `cmd help:"List topics"`
}

type DescribeTopicCmd struct {
	Name string `arg required help:"Topic name"`
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

type ListTopicsCmd struct{}

func (cmd *ListTopicsCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	kafkadmin := NewKafkaAdmin(config.Connections.Kafka)
	topics := kafkadmin.ListTopics()
	for name := range topics {
		fmt.Println(name)
	}
	return nil
}
