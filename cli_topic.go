package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/jedib0t/go-pretty/v6/table"
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

type ListTopicsCmd struct {
	OutputFormat string `default:"plain" help:"Output format: plain, table, json, detailed"`
	ShowInternal bool   `default:"false" help:"Show internal topics (starting with _)"`
	Pattern      string `help:"Filter topics by name pattern (regex)"`
}

type TopicListItem struct {
	Name              string            `json:"name"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int16             `json:"replication_factor"`
	Configs           map[string]string `json:"configs,omitempty"`
}

func (cmd *ListTopicsCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	kafkadmin := NewKafkaAdmin(config.Connections.Kafka)
	topics := kafkadmin.ListTopics()

	var pattern *regexp.Regexp
	var err error
	if cmd.Pattern != "" {
		pattern, err = regexp.Compile(cmd.Pattern)
		if err != nil {
			return fmt.Errorf("invalid pattern: %w", err)
		}
	}

	var topicList []TopicListItem
	for name, details := range topics {
		if !cmd.ShowInternal && strings.HasPrefix(name, "_") {
			continue
		}
		if pattern != nil && !pattern.MatchString(name) {
			continue
		}

		configs := make(map[string]string)
		for key, value := range details.ConfigEntries {
			if value != nil {
				configs[key] = *value
			}
		}

		topicList = append(topicList, TopicListItem{
			Name:              name,
			Partitions:        details.NumPartitions,
			ReplicationFactor: details.ReplicationFactor,
			Configs:           configs,
		})
	}

	sort.Slice(topicList, func(i, j int) bool {
		return topicList[i].Name < topicList[j].Name
	})

	switch cmd.OutputFormat {
	case "plain":
		cmd.renderPlain(topicList)
	case "table":
		cmd.renderTable(topicList)
	case "json":
		cmd.renderJSON(topicList)
	case "detailed":
		cmd.renderDetailed(topicList)
	default:
		return fmt.Errorf("unknown output format: %s (use plain, table, json, or detailed)", cmd.OutputFormat)
	}

	return nil
}

func (cmd *ListTopicsCmd) renderPlain(topics []TopicListItem) {
	for _, topic := range topics {
		fmt.Println(topic.Name)
	}
}

func (cmd *ListTopicsCmd) renderTable(topics []TopicListItem) {
	tb := table.NewWriter()
	tb.SetStyle(table.StyleLight)
	tb.SetOutputMirror(os.Stdout)
	tb.AppendHeader(table.Row{"Topic", "Partitions", "Replication Factor"})

	for _, topic := range topics {
		tb.AppendRow(table.Row{topic.Name, topic.Partitions, topic.ReplicationFactor})
	}

	tb.Render()
}

func (cmd *ListTopicsCmd) renderJSON(topics []TopicListItem) {
	jsonData, err := json.MarshalIndent(topics, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal topics to JSON: %s", err)
	}
	fmt.Println(string(jsonData))
}

func (cmd *ListTopicsCmd) renderDetailed(topics []TopicListItem) {
	for i, topic := range topics {
		if i > 0 {
			fmt.Println()
		}
		fmt.Printf("Topic: %s\n", topic.Name)
		fmt.Printf("  Partitions: %d\n", topic.Partitions)
		fmt.Printf("  Replication Factor: %d\n", topic.ReplicationFactor)
		if len(topic.Configs) > 0 {
			fmt.Println("  Configs:")
			var configKeys []string
			for key := range topic.Configs {
				configKeys = append(configKeys, key)
			}
			sort.Strings(configKeys)
			for _, key := range configKeys {
				fmt.Printf("    %s: %s\n", key, topic.Configs[key])
			}
		}
	}
}
