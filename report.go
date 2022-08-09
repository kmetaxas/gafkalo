package main

import (
	//"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"text/template"
)
import _ "embed"

//go:embed templates/console.tpl
var consoleTmplData string

func NewReport(topicResults []TopicResult, schemaResults []SchemaResult, clientResults []ClientResult, connectResults []ConnectorResult, isPlan bool) {

	var context Results
	context.Topics = topicResults
	context.Schemas = schemaResults
	context.Clients = clientResults
	context.Connectors = connectResults
	context.IsPlan = isPlan

	tmpl := template.Must(template.New("console").Parse(consoleTmplData))
	err := tmpl.Execute(os.Stdout, context)
	if err != nil {
		log.Fatal(err)
	}
}
