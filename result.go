package main

import (
	"github.com/IBM/sarama"
)

type Results struct {
	Topics           []TopicResult
	Schemas          []SchemaResult
	Clients          []ClientResult
	Connectors       []ConnectorResult
	ClusterLinks     []ClusterLinkResult
	IsPlan           bool
	ExtraContextKeys map[string]string // Used to pass extra context keys for use by templates
}

type ClusterLinkResult struct {
	Name       string
	Status     string // "Created", "Updated", "NoChange", "Error"
	Configs    map[string]string
	OldConfigs map[string]string
	Changes    *ClusterLinkConfigDiff
	Error      error
}

type TopicResult struct {
	Name                 string
	NewPartitions        int32 // can be compared with OldPartitions to see if changed
	OldPartitions        int32
	ReplicaPlan          [][]int32 // Filled in if partitions changed or replicationfactor changed. Represents the new replica assignment plan
	NewReplicationFactor int16     // Can be compared with OldReplicationFactor to see if changed
	OldReplicationFactor int16
	NewConfigs           map[string]*string // Contains only 'changed' configs (all of new topic)
	OldConfigs           map[string]*string // Old configs. Can be compared with NewConfigs.
	Errors               []string           // List of errors reported
	IsNew                bool               // NEwly created. Not expected to have anything old
}

type SchemaResult struct {
	SubjectName          string
	NewVersion           int      // New Version registered
	Changed              bool     // Will be true of subject was created or updated
	NewCompat            string   // Will be set if compatibility changed for this Subjec
	CompatibilityChecked bool     // Whether compatibility was checked before registration
	IsCompatible         bool     // Result of compatibility check
	CompatibilityLevel   string   // Compatibility level used for the check
	CompatibilityErrors  []string // Detailed reasons for incompatibility
}

type ClientResult struct {
	Principal    string
	ResourceType string
	ResourceName string
	Role         string
	PatternType  string // LITERAL Or PREFIXED
}

type ConnectorResult struct {
	Name       string
	NewConfigs map[string]string // New Configs
	OldConfigs map[string]string // Previous configs
	// key is field name, value is list of strings with errors returned by Validate API
	Errors          map[string][]string
	SensitiveFields map[string]bool // Fields that are sensitive (returned as asterisks from API)
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
		// We only care about changed conf values
		if *confVal == oldVal {
			continue
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

func (tr *TopicResult) HasChangedConfigs() bool {
	return len(tr.ChangedConfigs()) > 0
}

// Does the TopicResult have errors
func (tr *TopicResult) HasErrors() bool {
	if len(tr.Errors) > 0 {
		return true
	} else {
		return false
	}
}

func (tr *TopicResult) PartitionsChanged() bool {
	if (tr.NewPartitions != tr.OldPartitions) && !tr.IsNew {
		return true
	}
	return false
}

// Check if result has new compatibility set
func (res *SchemaResult) HasNewCompatibility() bool {
	return res.NewCompat != ""
}

func (res *SchemaResult) HasNewVersion() bool {
	if res.NewVersion != 0 {
		return true
	} else {
		return false
	}
}

func (res *SchemaResult) HasCompatibilityCheck() bool {
	return res.CompatibilityChecked
}

func (res *SchemaResult) IsSchemaCompatible() bool {
	return res.IsCompatible
}

func (res *SchemaResult) HasCompatibilityErrors() bool {
	return len(res.CompatibilityErrors) > 0
}

type ChangedConnectorConfig struct {
	Name        string
	OldVal      string
	NewVal      string
	IsSensitive bool
}

func (cr *ConnectorResult) ChangedConfigs() []ChangedConnectorConfig {
	var res []ChangedConnectorConfig

	for confName, confVal := range cr.NewConfigs {
		oldVal, exists := cr.OldConfigs[confName]
		if !exists {
			oldVal = ""
		}

		if confVal == oldVal {
			continue
		}

		isSensitive := false
		if cr.SensitiveFields != nil {
			isSensitive = cr.SensitiveFields[confName]
		}

		changedConf := ChangedConnectorConfig{
			Name:        confName,
			NewVal:      confVal,
			OldVal:      oldVal,
			IsSensitive: isSensitive,
		}
		res = append(res, changedConf)
	}
	return res
}

// ChangedClusterLinkConfig represents a changed configuration in a cluster link
type ChangedClusterLinkConfig struct {
	Name   string
	OldVal string
	NewVal string
}

// ChangedConfigs returns the list of changed configurations for a cluster link
func (clr *ClusterLinkResult) ChangedConfigs() []ChangedClusterLinkConfig {
	var res []ChangedClusterLinkConfig

	// For new links, all specified configs are considered new.
	if clr.Status == "Created" {
		for name, value := range clr.Configs {
			res = append(res, ChangedClusterLinkConfig{Name: name, NewVal: value, OldVal: ""})
		}
		return res
	}

	// For updated links, use the stored diff.
	if clr.Status == "Updated" && clr.Changes != nil {
		for name, change := range clr.Changes.ChangedConfigs {
			newItem := ChangedClusterLinkConfig{Name: name}
			if change.OldValue != nil {
				newItem.OldVal = *change.OldValue
			}
			if change.NewValue != nil {
				newItem.NewVal = *change.NewValue
			}
			res = append(res, newItem)
		}
	}

	return res
}

// HasChangedConfigs returns true if the cluster link has any config changes
func (clr *ClusterLinkResult) HasChangedConfigs() bool {
	// For new links, any config is a change.
	if clr.Status == "Created" {
		return len(clr.Configs) > 0
	}
	// For updated links, check if a diff was stored.
	if clr.Status == "Updated" && clr.Changes != nil {
		return len(clr.Changes.ChangedConfigs) > 0
	}
	return false
}
