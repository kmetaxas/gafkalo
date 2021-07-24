package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
	"math/rand"
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

func createTlsConfig(CAPath string, SkipVerify bool) *tls.Config {
	// Get system Cert Pool
	config := &tls.Config{}
	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		log.Fatal(err)
	}
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}
	pem, err := ioutil.ReadFile(CAPath)
	if err != nil {
		log.Fatal(err)
	}
	if ok := rootCAs.AppendCertsFromPEM(pem); !ok {
		log.Fatalf("Could not append cert %s to CertPool\n", CAPath)
	}
	config.RootCAs = rootCAs
	config.InsecureSkipVerify = SkipVerify
	return config

}

func SaramaConfigFromKafkaConfig(conf KafkaConfig) *sarama.Config {
	config := sarama.NewConfig()
	config.Metadata.Full = true
	config.Net.TLS.Enable = conf.SSL.Enabled
	if conf.Krb5.Enabled {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
		config.Net.SASL.GSSAPI.Realm = conf.Krb5.Realm
		config.Net.SASL.GSSAPI.Username = conf.Krb5.Username
		if conf.Krb5.Keytab != "" {
			config.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
			config.Net.SASL.GSSAPI.KeyTabPath = conf.Krb5.Keytab

		} else {
			config.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
			config.Net.SASL.GSSAPI.Password = conf.Krb5.Password
		}
		if conf.Krb5.ServiceName == "" {
			config.Net.SASL.GSSAPI.ServiceName = "kafka"
		} else {
			config.Net.SASL.GSSAPI.ServiceName = conf.Krb5.ServiceName
		}
		if conf.Krb5.KerberosConfigPath == "" {
			config.Net.SASL.GSSAPI.KerberosConfigPath = "/etc/krb5.conf"
		} else {
			config.Net.SASL.GSSAPI.KerberosConfigPath = conf.Krb5.KerberosConfigPath
		}
	}
	if conf.SSL.Enabled && (conf.SSL.CA != "" || conf.SSL.SkipVerify) {
		tlsConfig := createTlsConfig(conf.SSL.CA, conf.SSL.SkipVerify)
		config.Net.TLS.Config = tlsConfig
	}
	if conf.Producer.MaxMessageBytes != 0 {
		config.Producer.MaxMessageBytes = conf.Producer.MaxMessageBytes
	}
	if conf.Producer.Compression != "" {
		switch conf.Producer.Compression {
		case "snappy":
			config.Producer.Compression = sarama.CompressionSnappy
		case "gzip":
			config.Producer.Compression = sarama.CompressionGZIP
		case "lz4":
			config.Producer.Compression = sarama.CompressionLZ4
		case "zstd":
			config.Producer.Compression = sarama.CompressionZSTD
		case "none":
			config.Producer.Compression = sarama.CompressionNone
		}
	} else {
		// Use snappy as default if none other is specified
		config.Producer.Compression = sarama.CompressionSnappy
	}
	return config

}
func NewKafkaAdmin(conf KafkaConfig) KafkaAdmin {

	var admin KafkaAdmin
	config := SaramaConfigFromKafkaConfig(conf)

	saramaAdmin, err := sarama.NewClusterAdmin(conf.Brokers, config)
	if err != nil {
		log.Fatalf("Failed to create adminclient with: %s\n", err)
	}
	admin.AdminClient = saramaAdmin
	return admin

}

// Return a list of Kafka topics and fill cache.
func (admin *KafkaAdmin) ListTopics() map[string]sarama.TopicDetail {
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
			if *newVal != *oldVal {
				diff = append(diff, name)
			}
		}
	}
	return diff
}

// Test if a Topic's config need updating
func topicConfigNeedsUpdate(topic Topic, existing sarama.TopicDetail) bool {
	diff := getTopicConfigDiff(topic, existing)
	return len(diff) > 0
}

func topicPartitionNeedUpdate(topic Topic, existing sarama.TopicDetail) bool {
	return topic.Partitions != existing.NumPartitions
}

// Compare the topic names and give back a list of string on which topics are new and need to be created
func getTopicNamesDiff(oldTopics *map[string]sarama.TopicDetail, newTopics *map[string]Topic) []string {
	var newNames []string
	for name := range *newTopics {
		_, exists := (*oldTopics)[name]
		if !exists {
			newNames = append(newNames, name)
		}
	}
	return newNames
}

// Changes the partition count. Automatically calculates a re-assignment plan.
// Returns the new plan
func (admin *KafkaAdmin) ChangePartitionCount(topic string, count int32, replicationFactor int16, dry_run bool) ([][]int32, error) {
	var numBrokers int
	topicMetadata, err := admin.AdminClient.DescribeTopics([]string{topic})
	if err != nil {
		return nil, err
	}
	brokers, _, err := admin.AdminClient.DescribeCluster()
	numBrokers = len(brokers)
	if err != nil {
		return nil, err
	}
	var oldPlan [][]int32
	for _, partition := range topicMetadata[0].Partitions {
		oldPlan = append(oldPlan, partition.Replicas)
	}
	if len(oldPlan) > int(count) {
		return nil, errors.New("decreasing partition number is not possible in Kafka")
	}
	newPlan, err := calculatePartitionPlan(int32(int(count)-len(oldPlan)), numBrokers, replicationFactor, nil)
	if err != nil {
		return nil, err
	}
	if !dry_run {
		err = admin.AdminClient.CreatePartitions(topic, count, newPlan, false)
		if err != nil {
			return nil, err
		}
	}
	return newPlan, nil
}

/// Generate a new partitioning plan. If oldPlan is provided then respect that.
// if oldPlan is nil then it creates a plan for the requested count.
// if count == len(oldPlan) then a new plan is created (respecting oldPlan if possible). This is typicaly to modify replication factor
// If count != len(oldPlan) That is an error
func calculatePartitionPlan(count int32, numBrokers int, replicationFactor int16, oldPlan [][]int32) ([][]int32, error) {
	var newPlan [][]int32
	if oldPlan != nil && int(count) != len(oldPlan) {
		return newPlan, fmt.Errorf("can't calculate partition plan as count %d != length of old plan (%d)", count, len(oldPlan))
	}
	// Generate
	if oldPlan == nil {
		for i := 0; i < (int(count) - len(oldPlan)); i++ {
			var replicas []int32
			for b := 0; b < numBrokers; b++ {
				replicas = append(replicas, int32(rand.Intn(int(numBrokers))))
			}
			newPlan = append(newPlan, replicas)
		}
	} else {
		for _, part := range oldPlan {
			var newParts []int32
			switch curLen := int16(len(part)); {
			case curLen == replicationFactor:
				newParts = part[:replicationFactor]
			case curLen < replicationFactor:
				newParts = part
			case curLen > replicationFactor:
				{
					brokerIsTaken := make(map[int32]bool)
					for _, taken := range part {
						brokerIsTaken[taken] = true
					}
					for i := 0; i < numBrokers; i++ {
						brokerIsTaken[int32(i)] = true
					}
					var availableSet []int32
					for i := 0; i < numBrokers; i++ {
						if taken := brokerIsTaken[int32(i)]; !taken {
							availableSet = append(availableSet, int32(i))
						}
					}
					rand.Shuffle(len(availableSet), func(i, j int) {
						availableSet[i], availableSet[j] = availableSet[j], availableSet[i]
					})
					newParts = append(part, availableSet[:int(replicationFactor-int16(len(part)))]...)
				}
			}
			newPlan = append(newPlan, newParts)
		}
	}
	return newPlan, nil
}

// Reconcile actual with desired state
func (admin *KafkaAdmin) ReconcileTopics(topics map[string]Topic, dry_run bool) []TopicResult {

	// Get topics which are to be created
	var topicResults []TopicResult
	existing_topics := admin.ListTopics()
	newTopicsStatus := make(map[string]bool) // for each topic name if it failed or succeeded creation
	newTopics := getTopicNamesDiff(&existing_topics, &topics)
	// Initialize newTopicsStatus to false
	for _, name := range newTopics {
		newTopicsStatus[name] = false
	}

	// Create new topics
	for _, topicName := range newTopics {
		topic := topics[topicName]
		topicRes := TopicResultFromTopic(topic)
		topicRes.IsNew = true
		detail := sarama.TopicDetail{NumPartitions: topic.Partitions, ReplicationFactor: topic.ReplicationFactor, ConfigEntries: topic.Configs}
		err := admin.AdminClient.CreateTopic(topic.Name, &detail, dry_run)
		if err != nil {
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
					topicRes.Errors = append(topicRes.Errors, err.Error())
				}
				topicRes.NewConfigs = topic.Configs
				topicResults = append(topicResults, topicRes)
			}
			if topicPartitionNeedUpdate(topic, existing_topics[topicName]) {
				newPlan, err := admin.ChangePartitionCount(topicName, topic.Partitions, topic.ReplicationFactor, dry_run)
				if err != nil {
					topicRes.Errors = append(topicRes.Errors, err.Error())
				}
				topicRes.NewPartitions = topic.Partitions
				topicRes.OldPartitions = existing_topics[topicName].NumPartitions
				topicRes.ReplicaPlan = newPlan
				topicResults = append(topicResults, topicRes)
			}
		}
	}
	// TODO we currently don' update replicationFactor for existing topics. Fix that
	return topicResults
}
