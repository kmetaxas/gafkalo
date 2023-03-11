package main

import (
	"strings"
	"testing"
)

func createTestTopic() *Topic {
	min_insync_replicas := "2"
	retention_ms := "640000000"
	topic := Topic{
		Name:              "skata",
		Partitions:        2,
		ReplicationFactor: 2,
		Configs: map[string]*string{
			"min.insync.replicas": &min_insync_replicas,
			"retention.ms":        &retention_ms,
		},
	}
	return &topic

}
func TestGetRules(t *testing.T) {
	rules := GetRules()
	if len(rules) < 1 {
		t.Error("No rules in RuleFuncs found")
	}
}

func TestLintTopic(t *testing.T) {
	topic := createTestTopic()
	lintResults := LintTopic(*topic)
	if len(lintResults) < 1 {
		t.Errorf("Linter found no errors on topic %v but it is known to have errors", topic)
	}
}

func TestLintRuleIsReplication(t *testing.T) {
	topic := createTestTopic()
	res, hasRes := LintRuleIsReplication(*topic)
	if !hasRes {
		t.Errorf("LintRuleIsReplication should return a LintResult for %v", topic)
	}
	// We know that this topic has replication factor of 2 so we must get a warning
	if !strings.EqualFold(res.Severity, LINT_WARN) {
		t.Errorf("LINT: replication of 2 for topic [%v] should be a WARNING. We got [%v]", topic, res)
	}
	// Now chekc with replication of 1
	topic.ReplicationFactor = 1
	res, hasRes = LintRuleIsReplication(*topic)
	if !hasRes {
		t.Errorf("LintRuleIsReplication should return a LintResult for %v", topic)
	}
	// We know that this topic has replication factor of 2 so we must get a warning
	if res.Severity != LINT_ERROR {
		t.Error("LINT: replication of 1 should be a ERROR")
	}
	// Lets test a no-error scenario
	topic.ReplicationFactor = 3
	res, hasRes = LintRuleIsReplication(*topic)
	if hasRes {
		t.Errorf("LintRuleIsReplication found Lint results [%v] when it should not for topic %v", res, topic)
	}

}

func TestLintRuleMinIsr(t *testing.T) {
	topic := createTestTopic()
	res, hasRes := LintRuleMinIsr(*topic)
	if !hasRes {
		t.Errorf("No Lint results found for topic [%v] res=[%v]", topic, res)
	}
	topic.ReplicationFactor = 3
	res, hasRes = LintRuleMinIsr(*topic)
	if hasRes {
		t.Errorf("Found LintResult when it should not for topic [%v] res=[%v]", topic, res)
	}
}
