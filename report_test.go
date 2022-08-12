package main

import "testing"

func TestNewReport(t *testing.T) {
	var topic_results []TopicResult
	var schema_results []SchemaResult
	var client_results []ClientResult

	topic_res1 := TopicResult{
		Name:          "Topic1",
		NewPartitions: 1,
		OldPartitions: 1,
		IsNew:         true,
	}
	topic_results = append(topic_results, topic_res1)
	schema_res1 := SchemaResult{
		SubjectName: "Subject1",
		Changed:     true,
		NewCompat:   "FULL",
	}
	schema_res2 := SchemaResult{
		SubjectName: "Subject2",
		Changed:     true,
		NewVersion:  2,
	}
	schema_results = append(schema_results, schema_res1)
	schema_results = append(schema_results, schema_res2)
	NewReport(topic_results, schema_results, client_results, false)
	NewReport(topic_results, schema_results, client_results, true)
}
