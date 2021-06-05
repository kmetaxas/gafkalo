package main

import (
	"fmt"
	"testing"
)

func getTopic() Topic {
	return Topic{Name: "TestTopic", Partitions: 2, ReplicationFactor: 2}
}
func getTopicResult(changed bool, errors bool) TopicResult {
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
	tr := getTopicResult(false, false)
	diff := tr.ChangedConfigs()
	if len(diff) != 0 {
		t.Error("ChangedConfigs[] != 0")
	}
	tr = getTopicResult(true, false)
	diff = tr.ChangedConfigs()
	if len(diff) == 0 {
		t.Error(fmt.Sprintf("No changed configs detected:%+vs", tr))
	}

}

func TestHasChangedConfigs(t *testing.T) {
	tr := getTopicResult(false, false)
	if tr.HasChangedConfigs() {
		t.Error("TopicResult should not have changed configs")
	}
	tr = getTopicResult(true, false)
	if !tr.HasChangedConfigs() {
		t.Error("TopicResult should have changed configs")
	}
}

func TestHasErrors(t *testing.T) {
	tr := getTopicResult(false, false)
	if tr.HasErrors() {
		t.Error("TopicResult should not have errors")
	}
	tr = getTopicResult(false, true)
	if !tr.HasErrors() {
		t.Error("TopicResult should have errors")
	}

}

func TestPartitionsChanged(t *testing.T) {
	tr := getTopicResult(false, false)
	tr.NewPartitions = 5
	tr.OldPartitions = 2
	changed := tr.PartitionsChanged()
	if !changed {
		t.Error("Partitions should be detected as changed")
	}
}
