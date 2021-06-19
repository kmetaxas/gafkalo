package main

import (
	"log"
	"os"
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

var rules [](func(topic Topic) (*LintResult, bool))

// Will disover and load rules
func GetRules() {
	rules = append(rules, LintRuleIsReplication)
}

func NewLintResult(topic Topic) *LintResult {
	var res LintResult
	res.Topic = topic.Name
	return &res

}
func LintTopic(topic Topic) []LintResult {
	var results []LintResult
	GetRules()
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
