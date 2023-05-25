package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Make it global so that it can be accessed in callbacks, though its not a great solution
var gafkaloConfig Configuration

type CLIContext struct {
	Config string `required arg help:"configuration file"`
}
type PlanCmd struct {
	Dryrun bool `default:"true" hidden`
}

type ApplyCmd struct {
	Dryrun bool `default:"false" hidden`
}
type LintCmd struct {
}

type LintBrokersCmd struct {
	OnlyErrors bool `flag default:"false" `
}

var CLI struct {
	Config     string         `required help:"configuration file"`
	Verbosity  string         `help:"Verbosity level. error,info,debug,trace" default:"error"`
	Apply      ApplyCmd       `cmd help:"Apply the changes"`
	Plan       PlanCmd        `cmd help:"Produce a plan of changes"`
	Consumer   ConsumerCmd    `cmd help:"Consume from topics"`
	Produce    ProduceCmd     `cmd help:"Produce to a topic"`
	Schema     SchemaCmd      `cmd help:"Manage schemas"`
	Lint       LintCmd        `cmd help:"Run a linter against topic definitions"`
	Topic      TopicCmd       `cmd help:"Manage topics"`
	LintBroker LintBrokersCmd `cmd help:"Run a linter against topics in a running brokers"`
	Connect    ConnectCmd     `cmd help:"manage connectors"`
	//Consumergroup ConsumerGroupCmd `cmd help:"manage and view consumer groups"`
	Replicator ReplicatorCmd `cmd helm:"Replicator topics"`
}

func (cmd *ApplyCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	inputData := GetInputData(config)
	kafkadmin, sradmin, mdsadmin, connectAdmin := GetAdminClients(config)
	report := DoSync(&kafkadmin, &sradmin, &mdsadmin, &connectAdmin, &inputData, false)
	report.SetExtraContextKey("sensitive_regex", config.Kafkalo.ConnectorsSensitiveKeysRegex)
	report.Render(os.Stdout)
	return nil
}
func (cmd *PlanCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	inputData := GetInputData(config)
	kafkadmin, sradmin, mdsadmin, connectAdmin := GetAdminClients(config)
	report := DoSync(&kafkadmin, &sradmin, &mdsadmin, &connectAdmin, &inputData, true)
	report.SetExtraContextKey("sensitive_regex", config.Kafkalo.ConnectorsSensitiveKeysRegex)
	report.Render(os.Stdout)
	return nil
}
func parseOffsetsArg(arg *string) (map[int32]int64, error) {
	offsets := make(map[int32]int64)
	splitStrings := strings.Split(*arg, "=")
	if len(splitStrings) != 2 {
		return offsets, fmt.Errorf("= not found in SetOffsets param. Expected TOPIC=partition:offset")
	}
	// topic not used so assign to blank var
	_, offsetStr := splitStrings[0], splitStrings[1]
	partOffsetPairs := strings.Split(offsetStr, ",")
	for _, partOffsetPair := range partOffsetPairs {
		// Split into partition and offset and merge into offsets map
		splitStrings = strings.Split(partOffsetPair, ":")
		if len(splitStrings) != 2 {
			return offsets, fmt.Errorf("expected format partition:offset. Found: %s", partOffsetPair)
		}
		partition, err := strconv.ParseInt(splitStrings[0], 10, 32)
		if err != nil {
			return offsets, err
		}
		offset, err := strconv.ParseInt(splitStrings[1], 10, 64)
		if err != nil {
			return offsets, err
		}
		offsets[int32(partition)] = offset
	}
	return offsets, nil

}

func (cmd *LintCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	inputData := GetInputData(config)
	var results []LintResult
	for _, topic := range inputData.Topics {
		res := LintTopic(topic)
		results = append(results, res...)
	}
	PrettyPrintLintResults(results)
	return nil
}

/*
Run the Lint checkers against a kafka cluster's configured topics
This is useful, for example, to do a sanity check on an existing running cluster
*/
func (cmd *LintBrokersCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	var results []LintResult
	kafkadmin := NewKafkaAdmin(config.Connections.Kafka)
	existing_topics := kafkadmin.ListTopics()
	for _, topic := range existing_topics {
		res := LintTopic(topic)
		if cmd.OnlyErrors {
			for _, lintResult := range res {
				if lintResult.Severity == LINT_ERROR {
					results = append(results, lintResult)
				}
			}
		} else {
			results = append(results, res...)
		}
	}
	PrettyPrintLintResults(results)
	return nil
}
func LoadConfig(config string) Configuration {
	configuration := parseConfig(config)
	// set global variable
	gafkaloConfig = Configuration(configuration)
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

func GetAdminClients(config Configuration) (KafkaAdmin, SRAdmin, MDSAdmin, ConnectAdmin) {
	kafkadmin := NewKafkaAdmin(config.Connections.Kafka)
	sradmin := NewSRAdmin(&config)
	mdsadmin := new(MDSAdmin)
	if config.Connections.Mds != (MDSConfig{}) {
		mdsadmin = NewMDSAdmin(config.Connections.Mds)
	}
	connectAdmin := new(ConnectAdmin)
	if config.Connections.Connect != (ConnectConfig{}) {
		newConnectAdmin, err := NewConnectAdmin(&config.Connections.Connect)
		connectAdmin = newConnectAdmin
		if err != nil {
			log.Fatalf("Failed to create ConnectAdmin instance: %s", err)
		}
	}
	return kafkadmin, sradmin, *mdsadmin, *connectAdmin
}
func DoSync(kafkadmin *KafkaAdmin, sradmin *SRAdmin, mdsadmin *MDSAdmin, connectadmin *ConnectAdmin, inputData *DesiredState, dryRun bool) *Report {
	topicResults := kafkadmin.ReconcileTopics(inputData.Topics, dryRun)
	schemaResults := sradmin.Reconcile(inputData.Topics, dryRun)
	// Do MDS
	roleResults := mdsadmin.Reconcile(inputData.Clients, dryRun)
	connectResults := connectadmin.Reconcile(inputData.Connectors, dryRun)

	return NewReport(topicResults, schemaResults, roleResults, connectResults, dryRun)
}
