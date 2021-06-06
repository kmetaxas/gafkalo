package main

import (
	"fmt"
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
		t.Error(fmt.Sprintf("Res does not match topic (%+vs)", res))
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
		t.Error(fmt.Sprintf("No changed configs detected:%+vs", tr))
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
