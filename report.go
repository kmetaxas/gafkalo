package main

import (
	//"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"text/template"
)
import _ "embed"

//go:embed templates/console.tpl
var consoleTmplData string

type Report struct {
	Context  Results
	Template *template.Template
	IsPlan   bool
}

// Render and print in console
// TODO this should be more configurable
func (r *Report) Render(writer io.Writer) {
	err := r.Template.Execute(writer, r.Context)
	if err != nil {
		log.Fatal(err)
	}
}

func (r *Report) SetExtraContextKey(key string, value string) {
	if r.Context.ExtraContextKeys == nil {
		r.Context.ExtraContextKeys = make(map[string]string)
	}
	r.Context.ExtraContextKeys[key] = value
}

func NewReport(topicResults []TopicResult, schemaResults []SchemaResult, clientResults []ClientResult, connectResults []ConnectorResult, isPlan bool) *Report {
	var report Report
	var context Results
	context.Topics = topicResults
	context.Schemas = schemaResults
	context.Clients = clientResults
	context.Connectors = connectResults
	context.IsPlan = isPlan

	report.Context = context
	report.Template = template.Must(template.New("console").Parse(consoleTmplData))
	report.IsPlan = isPlan
	return &report
}
