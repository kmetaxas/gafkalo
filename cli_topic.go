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
	Describe   DescribeTopicCmd   `cmd help:"Describe topic"`
	List       ListTopicsCmd      `cmd help:"List topics"`
	Create     CreateTopicCmd     `cmd help:"Create topic"`
	Partitions PartitionsTopicCmd `cmd help:"Change partition count and replication factor"`
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

type CreateTopicCmd struct {
	Name              string            `short:"n" required help:"Topic name"`
	Partitions        int32             `default:"1" help:"Number of partitions"`
	ReplicationFactor int16             `name:"replication-factor" default:"1" help:"Replication factor"`
	Configs           map[string]string `short:"c" help:"Topic configurations (can be specified multiple times: -c key=value)"`
	ValidateOnly      bool              `default:"false" help:"Only validate the request without creating the topic"`
}

func (cmd *CreateTopicCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	kafkadmin := NewKafkaAdmin(config.Connections.Kafka)

	if cmd.Partitions < 1 {
		return fmt.Errorf("partitions must be at least 1, got %d", cmd.Partitions)
	}
	if cmd.ReplicationFactor < 1 {
		return fmt.Errorf("replication factor must be at least 1, got %d", cmd.ReplicationFactor)
	}

	existingTopics := kafkadmin.ListTopics()
	if _, exists := existingTopics[cmd.Name]; exists {
		return fmt.Errorf("topic '%s' already exists", cmd.Name)
	}

	configEntries := make(map[string]*string)
	for key, value := range cmd.Configs {
		v := value
		configEntries[key] = &v
	}

	topicDetail := &Topic{
		Name:              cmd.Name,
		Partitions:        cmd.Partitions,
		ReplicationFactor: cmd.ReplicationFactor,
		Configs:           configEntries,
	}

	err := kafkadmin.CreateTopic(topicDetail, cmd.ValidateOnly)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	if cmd.ValidateOnly {
		fmt.Printf("Topic '%s' configuration is valid (not created due to --validate-only)\n", cmd.Name)
	} else {
		fmt.Printf("Successfully created topic '%s'\n", cmd.Name)
		fmt.Printf("  Partitions: %d\n", cmd.Partitions)
		fmt.Printf("  Replication Factor: %d\n", cmd.ReplicationFactor)
		if len(cmd.Configs) > 0 {
			fmt.Println("  Configs:")
			var configKeys []string
			for key := range cmd.Configs {
				configKeys = append(configKeys, key)
			}
			sort.Strings(configKeys)
			for _, key := range configKeys {
				fmt.Printf("    %s: %s\n", key, cmd.Configs[key])
			}
		}
	}

	return nil
}

type PartitionsTopicCmd struct {
	Name    string `arg required help:"Topic name"`
	Count   int32  `required help:"New partition count"`
	Factor  int16  `required help:"Replication factor"`
	Plan    bool   `help:"Show plan without executing (dry run)"`
	Execute bool   `help:"Execute the partition change"`
}

func (cmd *PartitionsTopicCmd) Run(ctx *CLIContext) error {
	if !cmd.Plan && !cmd.Execute {
		return fmt.Errorf("must specify either --plan or --execute")
	}
	if cmd.Plan && cmd.Execute {
		return fmt.Errorf("cannot specify both --plan and --execute")
	}

	config := LoadConfig(ctx.Config)
	kafkadmin := NewKafkaAdmin(config.Connections.Kafka)

	topics := kafkadmin.ListTopics()
	topicDetail, exists := topics[cmd.Name]
	if !exists {
		return fmt.Errorf("topic '%s' does not exist", cmd.Name)
	}

	if cmd.Count < topicDetail.NumPartitions {
		return fmt.Errorf("cannot decrease partition count from %d to %d (Kafka does not support decreasing partitions)", topicDetail.NumPartitions, cmd.Count)
	}

	if cmd.Count < 1 {
		return fmt.Errorf("partition count must be at least 1, got %d", cmd.Count)
	}
	if cmd.Factor < 1 {
		return fmt.Errorf("replication factor must be at least 1, got %d", cmd.Factor)
	}

	dryRun := cmd.Plan
	newPlan, err := kafkadmin.ChangePartitionCount(cmd.Name, cmd.Count, cmd.Factor, dryRun)
	if err != nil {
		return fmt.Errorf("failed to change partition count: %w", err)
	}

	if cmd.Plan {
		fmt.Printf("Plan for topic '%s':\n", cmd.Name)
		fmt.Printf("  Current partitions: %d\n", topicDetail.NumPartitions)
		fmt.Printf("  New partitions: %d\n", cmd.Count)
		fmt.Printf("  Replication factor: %d\n", cmd.Factor)
		fmt.Println("\nNew partition assignments:")
		for i, replicas := range newPlan {
			fmt.Printf("  Partition %d: %v\n", int(topicDetail.NumPartitions)+i, replicas)
		}
		fmt.Println("\nTo execute this plan, run with --execute instead of --plan")
	} else {
		fmt.Printf("Successfully changed partitions for topic '%s'\n", cmd.Name)
		fmt.Printf("  Previous partitions: %d\n", topicDetail.NumPartitions)
		fmt.Printf("  New partitions: %d\n", cmd.Count)
		fmt.Printf("  Replication factor: %d\n", cmd.Factor)
	}

	return nil
}
