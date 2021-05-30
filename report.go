package main

import (
	//"fmt"
	"io/ioutil"
	"log"
	"os"
	"text/template"
)

func NewReport(topicResults []TopicResult) {

	var context Results
	context.Topics = topicResults

	tmplData, err := ioutil.ReadFile("templates/console.tpl")
	if err != nil {
		log.Fatal(err)
	}
	tmpl := template.Must(template.New("console").Parse(string(tmplData)))
	err = tmpl.Execute(os.Stdout, context)
	if err != nil {
		log.Fatal(err)
	}
}
