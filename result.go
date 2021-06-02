package main

import (
	"github.com/Shopify/sarama"
)

type Results struct {
	Topics  []TopicResult
	Schemas []SchemaResult
	Clients []ClientResult
	IsPlan  bool
}

type TopicResult struct {
	Name                 string
	NewPartitions        int32 // can be compared with OldPartitions to see if changed
	OldPartitions        int32
	NewReplicationFactor int16 // Can be compared with OldReplicationFactor to see if changed
	OldReplicationFactor int16
	NewConfigs           map[string]*string // Contains only 'changed' configs (all of new topic)
	OldConfigs           map[string]*string // Old configs. Can be compared with NewConfigs.
	Errors               []string           // List of errors reported
	IsNew                bool               // NEwly created. Not expected to have anything old

}

type SchemaResult struct {
	SubjectName string
	NewVersion  int    // New Version registered
	Changed     bool   // Will be true of subject was created or updated
	NewCompat   string // Will be set if compatibility changed for this Subjec
}

type ClientResult struct {
	Principal    string
	ResourceType string
	ResourceName string
	Role         string
	PatternType  string // LITERAL Or PREFIXED
}

func TopicResultFromTopic(topic Topic) TopicResult {
	return TopicResult{
		Name:                 topic.Name,
		NewPartitions:        topic.Partitions,
		NewReplicationFactor: topic.ReplicationFactor,
		NewConfigs:           topic.Configs,
		IsNew:                false, // Defaul value
	}
}

// Fill-in
func (tr *TopicResult) FillFromOldTopic(old sarama.TopicDetail) {
	tr.OldPartitions = old.NumPartitions
	tr.OldReplicationFactor = old.ReplicationFactor
	tr.OldConfigs = old.ConfigEntries
}

// A nice , easy way to process changes in configs
type ChangedConfig struct {
	Name   string
	OldVal string
	NewVal string
}

func (tr *TopicResult) ChangedConfigs() []ChangedConfig {
	var res []ChangedConfig

	for confName, confVal := range (*tr).NewConfigs {
		var oldVal string
		oldValP, exists := tr.OldConfigs[confName]
		if !exists {
			oldVal = ""
		} else {
			oldVal = *oldValP
		}
		changedConf := ChangedConfig{
			Name:   confName,
			NewVal: *confVal,
			OldVal: oldVal,
		}
		res = append(res, changedConf)
	}
	return res
}

// Does the TopicResult have errors
func (tr *TopicResult) HasErrors() bool {
	if len(tr.Errors) > 0 {
		return true
	} else {
		return false
	}
}