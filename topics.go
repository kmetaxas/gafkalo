package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
)

type Partition struct {
	TopicName       string
	Num             int32
	Leader          int32
	LeaderEpoch     int32
	ISR             []int32
	OfflineReplicas []int32
}
type Partitions map[int32]Partition
type Topics map[string]Topic

func (t *Topics) Names() []string {
	var names []string
	for topicName, _ := range *t {
		names = append(names, topicName)
	}
	return names
}

type Topic struct {
	Name              string             `yaml:"name"`
	Partitions        int32              `yaml:"partitions"`
	ReplicationFactor int16              `yaml:"replication_factor"`
	Configs           map[string]*string `yaml:"configs"`
	Key               Schema             `yaml:"key"`
	Value             Schema             `yaml:"value"`
	IsInternal        bool               `yaml:"isInternal"`
	ID                [16]byte           `yaml:"id"`
	PartitionDetails  Partitions         // This is not to be part of YAML
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
	AdminClient kadm.Client
	Consumer    string
	TopicCache  Topics
	DryRun      bool
	DryRunPlan  []TopicPlan
	Timeout     time.Duration
}

// Get a context to use. Uses KafkaAdmin Timeout if defined, or defaults to 15 seconds
func (admin *KafkaAdmin) NewContext() (context.Context, context.CancelFunc) {
	timeout := 15 * time.Second
	if admin.Timeout != 0 {
		timeout = admin.Timeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return ctx, cancel
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
	if CAPath != "" {
		pem, err := ioutil.ReadFile(CAPath)
		if err != nil {
			log.Fatal(err)
		}
		if ok := rootCAs.AppendCertsFromPEM(pem); !ok {
			log.Fatalf("Could not append cert %s to CertPool\n", CAPath)
		}
		log.Tracef("Created TLS Config from PEM %s (InsecureSkipVerify=%v)", pem, SkipVerify)
	}
	config.RootCAs = rootCAs
	config.InsecureSkipVerify = SkipVerify
	log.Tracef("Setting InsecureSkipVerify=%v", SkipVerify)
	return config

}

// Return a Gafkalo Data structure from a goadm datastructure
func NewGafkaloTopicsFromFranzTopics(topics kadm.TopicDetails) Topics {
	gafkaloTopics := make(Topics)

	for fName, fTopic := range topics {
		// create new topics and copy them
		newTopic := Topic{
			Name:       fName,
			ID:         fTopic.ID,
			IsInternal: fTopic.IsInternal,
			Partitions: int32(len(fTopic.Partitions)),
		}
		newTopic.PartitionDetails = *new(Partitions)
		for partNum, partDetails := range fTopic.Partitions {
			newTopic.PartitionDetails[partNum] = Partition{
				TopicName:       fName,
				Num:             partDetails.Partition,
				Leader:          partDetails.Leader,
				LeaderEpoch:     partDetails.LeaderEpoch,
				ISR:             partDetails.ISR,
				OfflineReplicas: partDetails.OfflineReplicas,
			}
		}
		// Now copy partition details
		newTopic.Configs = make(map[string]*string)
		gafkaloTopics[fTopic.Topic] = newTopic
	}

	return gafkaloTopics
}

func NewKafkaAdmin(conf KafkaConfig) KafkaAdmin {

	var admin KafkaAdmin
	// XXX replace this with something for franz-go
	//config := SaramaConfigFromKafkaConfig(conf)

	kgoClient, err := kgo.NewClient(
		//kgo.SeedBrokers(conf.Brokers...),
		kgo.SeedBrokers("localhost:9092"),
		kgo.MaxVersions(kversion.V2_4_0()),
		// TODO add all appropriate parms from conf somehow
	)
	if err != nil {
		log.Fatalf("Failed to create kgo client with: %s\n", err)
	}
	franzAdmin := kadm.NewClient(kgoClient)
	admin.AdminClient = *franzAdmin
	admin.Timeout = 15 * time.Second
	return admin

}

// Return a list of Kafka topics and fill cache.
func (admin *KafkaAdmin) ListTopics() Topics {
	ctx, cancel := admin.NewContext()
	defer cancel()
	log.Tracef("Calling list topics using client %v", admin.AdminClient)
	fTopics, err := admin.AdminClient.ListTopics(ctx)
	topics := NewGafkaloTopicsFromFranzTopics(fTopics)
	if err != nil {
		log.Fatalf("Failed to list topics with: %s\n", err)
	}
	log.Tracef("ListTopics = %v", topics)
	// franz-go does not do a describe when listing topics, that is a separate call. Lets do that
	log.Tracef("Describeconfigs using client %v", admin.AdminClient)
	resourceConfigs, err := admin.AdminClient.DescribeTopicConfigs(ctx, topics.Names()...)
	if err != nil {
		log.Fatalf("Failed to describe topic configs with error: %s", err)
	}

	// Copy the configs from franz-go to Gafkalo data structure
	for _, fConfig := range resourceConfigs {
		log.Tracef("Processing fConfig %v", fConfig)
		for _, conf := range fConfig.Configs {
			topics[fConfig.Name].Configs[conf.Key] = conf.Value

		}
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
func getTopicConfigDiff(newTopic Topic, oldTopic Topic) []string {
	var diff []string
	for name, newVal := range newTopic.Configs {
		if oldVal, exists := oldTopic.Configs[name]; exists {
			if *newVal != *oldVal {
				diff = append(diff, name)
			}
		}
	}
	return diff
}

// Test if a Topic's config need updating
func topicConfigNeedsUpdate(topic Topic, existing Topic) bool {
	diff := getTopicConfigDiff(topic, existing)
	return len(diff) > 0
}

func topicPartitionNeedUpdate(topic Topic, existing Topic) bool {
	return topic.Partitions != existing.Partitions
}

// Compare the topic names and give back a list of string on which topics are new and need to be created
func getTopicNamesDiff(oldTopics Topics, newTopics Topics) []string {
	var newNames []string
	for name := range newTopics {
		_, exists := (oldTopics)[name]
		if !exists {
			newNames = append(newNames, name)
		}
	}
	return newNames
}

// Changes the partition count. Automatically calculates a re-assignment plan.
// Returns the new plan
/*
func (admin *KafkaAdmin) ChangePartitionCount(topic string, count int32, replicationFactor int16, dry_run bool) ([][]int32, error) {
	var numBrokers int
	var brokerIDs []int32

	topicMetadata, err := admin.AdminClient.DescribeTopics([]string{topic})
	if err != nil {
		return nil, err
	}
	brokers, _, err := admin.AdminClient.DescribeCluster()
	if err != nil {
		return nil, err
	}
	numBrokers = len(brokers)
	var oldPlan [][]int32
	for _, partition := range topicMetadata[0].Partitions {
		oldPlan = append(oldPlan, partition.Replicas)
	}
	if len(oldPlan) > int(count) {
		return nil, errors.New("decreasing partition number is not possible in Kafka")
	}
	// Create a list of broker IDs for calculatePartitionPlan
	for _, brokerId := range brokers {
		brokerIDs = append(brokerIDs, brokerId.ID())
	}
	// Note, We subtract the existing partitions because we call CreatePartitions() to increase the partition count and we
	// only care about the *new* partitions, not the whole partitioning scheme of the topic
	newPlan, err := calculatePartitionPlan(int32(count-int32(len(oldPlan))), numBrokers, replicationFactor, brokerIDs, nil)
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
*/

/*
Generate a random set of integers of size `size` between `from` and `to` arguments (included)
that is non-repeating. Meaning it will not include a number twice. Obviously, `size` can't be larger than to-from
*/
func randNonRepeatingIntSet(brokerIDs []int32, size int) ([]int32, error) {
	var err error
	var result []int32
	if size > len(brokerIDs) {
		return result, fmt.Errorf("requested set size (%d) bigger than available cluster size of %d", size, len(brokerIDs))
	}
	//for i := from; i < to+1; i++ {
	rand.Shuffle(len(brokerIDs), func(i, j int) {
		brokerIDs[i], brokerIDs[j] = brokerIDs[j], brokerIDs[i]
	})
	result = brokerIDs[:size]
	//log.Printf("randNonRepeatingIntSet returning %v from a larger set of %v", result, numSet)
	return result, err

}

/// Generate a new partitioning plan. If oldPlan is provided then respect that.
// if oldPlan is nil then it creates a plan for the requested count.
// if count == len(oldPlan) then a new plan is created (respecting oldPlan if possible). This is typicaly to modify replication factor
// If count != len(oldPlan) That is an error
func calculatePartitionPlan(count int32, numBrokers int, replicationFactor int16, brokerIDs []int32, oldPlan [][]int32) ([][]int32, error) {
	var newPlan [][]int32
	if oldPlan != nil && int(count) != len(oldPlan) {
		return newPlan, fmt.Errorf("can't calculate partition plan as count %d != length of old plan (%d)", count, len(oldPlan))
	}
	// Generate
	if oldPlan == nil {
		if int(replicationFactor) > numBrokers {
			return newPlan, fmt.Errorf("can't have replication factor %d with only %d brokers", replicationFactor, numBrokers)
		}
		for i := 0; i < (int(count) - len(oldPlan)); i++ {
			var replicas []int32
			randomBrokerIDs, err := randNonRepeatingIntSet(brokerIDs, int(replicationFactor))
			if err != nil {
				return newPlan, err
			}
			for b := 0; b < int(replicationFactor); b++ {
				replicas = append(replicas, int32(randomBrokerIDs[b]))
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
	/*
		existing_topics := admin.ListTopics()
		newTopicsStatus := make(map[string]bool) // for each topic name if it failed or succeeded creation
		newTopics := getTopicNamesDiff(&existing_topics, &topics)
		log.Tracef("Topics to create %v (dry_run=%v)", newTopics, dry_run)
		// Initialize newTopicsStatus to false
		for _, name := range newTopics {
			newTopicsStatus[name] = false
		}

		log.Tracef("creating topics (dry_run=%v)", dry_run)
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
			log.Debugf("Create Topic %s - config %v (Dryrun %v)", topic.Name, detail, dry_run)
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
	*/
	return topicResults
}
