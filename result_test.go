package main

import (
	"testing"
)

func getTopic() Topic {
	return Topic{Name: "TestTopic", Partitions: 2, ReplicationFactor: 2}
}

func getTestClientResult() ClientResult {
	res := ClientResult{Principal: "User:testuser", ResourceType: "Topic", ResourceName: "TestTopic", Role: "DeveloperRead", PatternType: "LITERAL"}

	return res
}
func getTestTopicResult(changed bool, errors bool) TopicResult {
	topic := getTopic()
	tr := TopicResultFromTopic(topic)
	if changed {
		newval := "newvalue"
		oldval := "oldvalue"
		tr.OldConfigs = make(map[string]*string)
		tr.NewConfigs = make(map[string]*string)
		tr.NewConfigs["config1"] = &newval
		tr.OldConfigs["config1"] = &oldval
	}
	if errors {
		errors := []string{"ERROR1", "ERROR2"}
		tr.Errors = errors

	}
	return tr

}
func TestTopicResultFromTopic(t *testing.T) {
	topic := getTopic()
	res := TopicResultFromTopic(topic)
	if res.Name != topic.Name || res.NewPartitions != topic.Partitions || res.NewReplicationFactor != topic.ReplicationFactor {
		t.Errorf("Res does not match topic (%+vs)", res)
	}
}

func TestChangedConfigs(t *testing.T) {
	tr := getTestTopicResult(false, false)
	diff := tr.ChangedConfigs()
	if len(diff) != 0 {
		t.Error("ChangedConfigs[] != 0")
	}
	tr = getTestTopicResult(true, false)
	diff = tr.ChangedConfigs()
	if len(diff) == 0 {
		t.Errorf("No changed configs detected:%+vs", tr)
	}

}

func TestHasChangedConfigs(t *testing.T) {
	tr := getTestTopicResult(false, false)
	if tr.HasChangedConfigs() {
		t.Error("TopicResult should not have changed configs")
	}
	tr = getTestTopicResult(true, false)
	if !tr.HasChangedConfigs() {
		t.Error("TopicResult should have changed configs")
	}
}

func TestHasErrors(t *testing.T) {
	tr := getTestTopicResult(false, false)
	if tr.HasErrors() {
		t.Error("TopicResult should not have errors")
	}
	tr = getTestTopicResult(false, true)
	if !tr.HasErrors() {
		t.Error("TopicResult should have errors")
	}

}

func TestPartitionsChanged(t *testing.T) {
	tr := getTestTopicResult(false, false)
	tr.NewPartitions = 5
	tr.OldPartitions = 2
	changed := tr.PartitionsChanged()
	if !changed {
		t.Error("Partitions should be detected as changed")
	}
	tr.NewPartitions = 2
	tr.OldPartitions = 2
	changed = tr.PartitionsChanged()
	if changed {
		t.Error("Partitions NOT should be detected as changed")
	}
}

func TestConnectorChangedConfigs(t *testing.T) {
	cr := ConnectorResult{
		Name: "test-connector",
		NewConfigs: map[string]string{
			"config1": "newvalue1",
			"config2": "newvalue2",
			"config3": "samevalue",
		},
		OldConfigs: map[string]string{
			"config1": "oldvalue1",
			"config2": "oldvalue2",
			"config3": "samevalue",
		},
		SensitiveFields: map[string]bool{
			"config1": true,
		},
	}

	changed := cr.ChangedConfigs()
	if len(changed) != 2 {
		t.Errorf("Expected 2 changed configs, got %d", len(changed))
	}

	foundSensitive := false
	foundNonSensitive := false
	for _, c := range changed {
		if c.Name == "config1" && c.IsSensitive {
			foundSensitive = true
		}
		if c.Name == "config2" && !c.IsSensitive {
			foundNonSensitive = true
		}
	}

	if !foundSensitive {
		t.Error("Expected config1 to be marked as sensitive")
	}
	if !foundNonSensitive {
		t.Error("Expected config2 to be marked as non-sensitive")
	}
}
