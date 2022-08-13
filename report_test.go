package main

import "testing"

func TestNewReport(t *testing.T) {
	var topic_results []TopicResult
	var schema_results []SchemaResult
	var client_results []ClientResult
	var connect_results []ConnectorResult

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

	connect_res1 := ConnectorResult{
		Name:       "connector1",
		NewConfigs: map[string]string{"skata": "pola"},
		OldConfigs: map[string]string{"skata": "pola"},
	}
	connect_results = append(connect_results, connect_res1)

	NewReport(topic_results, schema_results, client_results, connect_results, false)
	NewReport(topic_results, schema_results, client_results, connect_results, true)
}
