package main

import (
	//"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"regexp"
	"text/template"
)
import _ "embed"

//go:embed templates/console.tpl
var consoleTmplData string

type Report struct {
	Context  Results
	Template *template.Template
	IsPlan   bool
	//	SensitivityFilter string
}

// Render and print in console
// TODO this should be more configurable, i.e emit json , html etc.
func (r *Report) Render(writer io.Writer) {
	err := r.Template.Execute(writer, r.Context)
	if err != nil {
		log.Fatal(err)
	}
}

/*
Takes a string (typically a configuration key) and a regex.
If the regex matches the key, then it is replaced by a sensitive info message
*/

func HideSensitiveKey(key, value, regex string) string {
	// if user did not provide a regex (empty string ) return value
	if regex == "" {
		return value
	}
	match, err := regexp.MatchString(regex, key)
	if err != nil {
		log.Errorf("Failed to match regex %s with key: %s, error: %s", regex, key, err)
		return value // Don't crash, return empty string
	}
	if match {
		return "(Sensitive info redacted)"
	}
	return value
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

	templateFunctions := template.FuncMap{
		"HideSensitive": HideSensitiveKey,
	}
	report.Template = template.Must(template.New("console").Funcs(templateFunctions).Parse(consoleTmplData))
	report.IsPlan = isPlan
	return &report
}
