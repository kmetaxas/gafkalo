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

func getTestSchemaResult(compatible bool, withErrors bool) SchemaResult {
	sr := SchemaResult{
		SubjectName:          "test-subject",
		NewVersion:           1,
		Changed:              true,
		NewCompat:            "BACKWARD",
		CompatibilityChecked: true,
		IsCompatible:         compatible,
		CompatibilityLevel:   "BACKWARD",
	}
	if withErrors {
		sr.CompatibilityErrors = []string{
			"Field removed: field1",
			"Incompatible type change: field2",
		}
	}
	return sr
}

func TestSchemaResultHasCompatibilityCheck(t *testing.T) {
	sr := getTestSchemaResult(true, false)
	if !sr.HasCompatibilityCheck() {
		t.Error("SchemaResult should have compatibility check")
	}

	sr.CompatibilityChecked = false
	if sr.HasCompatibilityCheck() {
		t.Error("SchemaResult should not have compatibility check")
	}
}

func TestSchemaResultIsSchemaCompatible(t *testing.T) {
	sr := getTestSchemaResult(true, false)
	if !sr.IsSchemaCompatible() {
		t.Error("SchemaResult should be compatible")
	}

	sr = getTestSchemaResult(false, false)
	if sr.IsSchemaCompatible() {
		t.Error("SchemaResult should not be compatible")
	}
}

func TestSchemaResultHasCompatibilityErrors(t *testing.T) {
	sr := getTestSchemaResult(false, false)
	if sr.HasCompatibilityErrors() {
		t.Error("SchemaResult should not have compatibility errors")
	}

	sr = getTestSchemaResult(false, true)
	if !sr.HasCompatibilityErrors() {
		t.Error("SchemaResult should have compatibility errors")
	}
	if len(sr.CompatibilityErrors) != 2 {
		t.Errorf("Expected 2 compatibility errors, got %d", len(sr.CompatibilityErrors))
	}
}

func TestSchemaResultHasNewCompatibility(t *testing.T) {
	sr := SchemaResult{
		SubjectName: "test-subject",
		NewCompat:   "FULL",
	}
	if !sr.HasNewCompatibility() {
		t.Error("SchemaResult should have new compatibility")
	}

	sr.NewCompat = ""
	if sr.HasNewCompatibility() {
		t.Error("SchemaResult should not have new compatibility")
	}
}

func TestSchemaResultHasNewVersion(t *testing.T) {
	sr := SchemaResult{
		SubjectName: "test-subject",
		NewVersion:  2,
	}
	if !sr.HasNewVersion() {
		t.Error("SchemaResult should have new version")
	}

	sr.NewVersion = 0
	if sr.HasNewVersion() {
		t.Error("SchemaResult should not have new version")
	}
}
