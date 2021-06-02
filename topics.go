package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
)

type Topic struct {
	Name              string             `yaml:"name"`
	Partitions        int32              `yaml:"partitions"`
	ReplicationFactor int16              `yaml:"replication_factor"`
	Configs           map[string]*string `yaml:"configs"`
	Key               Schema             `yaml:"key"`
	Value             Schema             `yaml:"value"`
}

// Dry run data for a Topic
type TopicPlan struct {
	Topic string
	// Any Reason given by the brokers for possible errors (for example not enough brokers for a replication factor etc)
	Reason string
	// An array of structs with a before and after key indicating the setting before and after the change for each Key
	ConfigDelta []struct {
		Key    string
		Before string
		After  string
	}
}
type KafkaAdmin struct {
	AdminClient sarama.ClusterAdmin
	Consumer    string
	TopicCache  map[string]sarama.TopicDetail
	DryRun      bool
	DryRunPlan  []TopicPlan
}

func NewKafkaAdmin() KafkaAdmin {

	var admin KafkaAdmin
	brokers := []string{"localhost:9093"}
	config := sarama.NewConfig()
	config.Metadata.Full = true
	config.Net.TLS.Enable = false

	saramaAdmin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create adminclient with: %s\n", err)
	}
	admin.AdminClient = saramaAdmin
	return admin

}

// Return a list of Kafka topics and fill cache.
func (admin KafkaAdmin) ListTopics() map[string]sarama.TopicDetail {
	topics, err := admin.AdminClient.ListTopics()
	if err != nil {
		log.Fatalf("Failed to list topics with: %s\n", err)
	}
	admin.TopicCache = topics
	return topics
}

// Unmarshal yaml callback for Topic
func (s *Topic) UnmarshalYAML(unmarshal func(interface{}) error) error {

	type rawTopic Topic
	raw := rawTopic{}
	if err := unmarshal(&raw); err != nil {
		return err
	}
	// Set key subject name
	if (Schema{} == raw.Key) {
	} else {
		raw.Key.SubjectName = raw.Name + "-key"
	}
	// Set Value subject name
	if (Schema{} == raw.Value) {
	} else {
		raw.Value.SubjectName = raw.Name + "-value"
	}

	*s = Topic(raw)
	return nil
}

// Compare two topic definitions of newTopic with oldTopic and give back a list of configs that are different from new to old
func getTopicConfigDiff(newTopic Topic, oldTopic sarama.TopicDetail) []string {
	var diff []string
	for name, newVal := range newTopic.Configs {
		if oldVal, exists := oldTopic.ConfigEntries[name]; exists {
			if newVal != oldVal {
				diff = append(diff, name)
			}
		}
	}
	return nil
}

// Test if a Topic's config need updating
func topicConfigNeedsUpdate(topic Topic, existing sarama.TopicDetail) bool {
	diff := getTopicConfigDiff(topic, existing)
	if len(diff) > 0 {
		return true
	}
	return false
}

// Compare the topic names and give back a list of string on which topics are new and need to be created
func getTopicNamesDiff(oldTopics *map[string]sarama.TopicDetail, newTopics *map[string]Topic) []string {
	var newNames []string
	for name, _ := range *newTopics {
		_, exists := (*oldTopics)[name]
		if !exists {
			newNames = append(newNames, name)
		}
	}
	return newNames
}

// Compare the topic config and give a TopicResult of what is expected to change
func getTopicDiff(oldTopic sarama.TopicDetail, newTopic Topic) *TopicResult {
	var topicDiff *TopicResult
	topicDiff.NewPartitions = newTopic.Partitions
	topicDiff.OldPartitions = oldTopic.NumPartitions
	topicDiff.NewReplicationFactor = newTopic.ReplicationFactor
	topicDiff.OldReplicationFactor = oldTopic.ReplicationFactor
	topicDiff.NewConfigs = newTopic.Configs
	topicDiff.OldConfigs = oldTopic.ConfigEntries
	return topicDiff
}

// Reconcile actual with desired state
func (admin KafkaAdmin) ReconcileTopics(topics map[string]Topic, dry_run bool) []TopicResult {

	// Get topics which are to be created
	var topicResults []TopicResult
	existing_topics := admin.ListTopics()
	newTopicsStatus := make(map[string]bool) // for each topic name if it failed or succeeded creation
	newTopics := getTopicNamesDiff(&existing_topics, &topics)
	// Initialize newTopicsStatus to false
	for _, name := range newTopics {
		newTopicsStatus[name] = false
	}

	fmt.Printf("We need to create the topics: %s\n", newTopics)
	// Create new topics
	for _, topicName := range newTopics {
		topic := topics[topicName]
		topicRes := TopicResultFromTopic(topic)
		topicRes.IsNew = true
		detail := sarama.TopicDetail{NumPartitions: topic.Partitions, ReplicationFactor: topic.ReplicationFactor, ConfigEntries: topic.Configs}
		err := admin.AdminClient.CreateTopic(topic.Name, &detail, dry_run)
		if err != nil {
			log.Printf("Creating topic failed with: %s\n", err)
			topicRes.Errors = append(topicRes.Errors, err.Error())
			newTopicsStatus[topicName] = false
		}
		topicResults = append(topicResults, topicRes)
		newTopicsStatus[topicName] = true
	}
	// Alter configs
	for topicName, topic := range topics {
		topicRes := TopicResultFromTopic(topic)
		topicRes.FillFromOldTopic(existing_topics[topicName])
		// skip topics we just created or topics that failed creation. So all new ones
		_, isNew := newTopicsStatus[topicName]
		if !isNew {
			if topicConfigNeedsUpdate(topic, existing_topics[topicName]) {
				err := admin.AdminClient.AlterConfig(sarama.TopicResource, topicName, topic.Configs, dry_run)
				if err != nil {
					log.Printf("Updating configs failed with: %s\n", err)
					topicRes.Errors = append(topicRes.Errors, err.Error())
				}
				topicRes.NewConfigs = topic.Configs
				topicResults = append(topicResults, topicRes)
			}
		}
	}
	// TODO we currently don't update partitions or replicationFactor for existing topics. Fix that
	return topicResults
}
