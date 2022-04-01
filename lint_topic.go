package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"text/template"
)
import _ "embed"

//go:embed templates/lintresult.tpl
var lintResultTmplData string

const (
	LINT_ERROR = "ERROR"
	LINT_WARN  = "WARNING"
	LINT_INFO  = "INFO"
)

type LintResult struct {
	Severity string //
	Message  string // The problem
	Topic    string // Which topic
	Hint     string // Propose a solution
}

type RuleFuncs [](func(topic Topic) (*LintResult, bool))

// Will disover and load rules
func GetRules() RuleFuncs {
	var rules RuleFuncs
	rules = append(rules, LintRuleIsReplication)
	rules = append(rules, LintRuleMinIsr)
	return rules
}

func NewLintResult(topic Topic) *LintResult {
	var res LintResult
	res.Topic = topic.Name
	return &res

}
func LintTopic(topic Topic) []LintResult {
	var results []LintResult
	rules := GetRules()
	for _, rule := range rules {
		res, hasRes := rule(topic)
		if hasRes {
			results = append(results, *res)
		}
	}
	return results
}

func LintRuleIsReplication(topic Topic) (*LintResult, bool) {
	res := NewLintResult(topic)
	if topic.ReplicationFactor < 2 {
		res.Message = "Replication factor < 2. Possible downtime"
		res.Hint = "Increase replication factor to 3"
		res.Severity = LINT_ERROR
		return res, true
	}
	if topic.ReplicationFactor < 3 {
		res.Message = "Replication factor < 3"
		res.Hint = LINT_WARN
		return res, true
	}
	return res, false
}

func LintRuleMinIsr(topic Topic) (*LintResult, bool) {
	res := NewLintResult(topic)
	if min_isr_conf, exists := topic.Configs["min.insync.replicas"]; exists {
		min_isr, err := strconv.ParseInt(*min_isr_conf, 10, 32)
		if err != nil {
			log.Fatal(nil)
		}
		log.Tracef("min.isr=%d\n", min_isr)
		// min_isr must be below replicationFactor or we can stop producers using acks=all
		if int64(topic.ReplicationFactor) <= min_isr {
			log.Debugf("Identified possible min isr issue for topic %s (minIsr:%d, replication:%d)", topic.Name, min_isr, topic.ReplicationFactor)
			res.Message = fmt.Sprintf("min.insync.replicas (%d) must be below replicationFactor (%d)", min_isr, topic.ReplicationFactor)
			res.Severity = LINT_ERROR
			res.Hint = "The min.insync.replicas setting *must* be below the replication factor for the topic. Otherwise a broker restart will stop production for consumers using acks=all"
			return res, true
		}
	} else {
		res.Message = "min.insync.replicas not defined"
		res.Severity = LINT_WARN
		res.Hint = "Setting min.insync.replicas to 2 or higher will reduce chances of data-loss"
		return res, true
	}
	return res, false
}

type LintTemplateContext struct {
	LintResults []LintResult
}

func PrettyPrintLintResults(results []LintResult) {

	var context LintTemplateContext
	context.LintResults = results
	tmpl := template.Must(template.New("lintresult").Parse(lintResultTmplData))
	err := tmpl.Execute(os.Stdout, context)
	if err != nil {
		log.Fatal(err)
	}

}
