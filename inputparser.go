package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

// This represents the desired state that the user asked for
// It a merge of all individual input files
type DesiredState struct {
	Topics  map[string]Topic
	Clients []Client
}

type InputYaml struct {
	Topics  []Topic  `yaml:"topics"`
	Clients []Client `yaml:"clients"`
}

func (state *DesiredState) mergeInput(data *InputYaml) error {
	for _, topic := range data.Topics {
		// make sure it doens not exist first!
		if val, ok := state.Topics[topic.Name]; ok {
			log.Fatalf("Duplicate definition of topic %+vs\n", val)
		}
		state.Topics[topic.Name] = topic
	}
	return nil
}
func Parse(inputFiles []string) DesiredState {
	var desiredState = DesiredState{Topics: make(map[string]Topic), Clients: make([]Client, 20)}
	for _, filename := range inputFiles {
		data, err := ioutil.ReadFile(filename)
		if err != nil {
			log.Printf("unable to read %s with error %s\n", filename, err)
		}
		var inputdata InputYaml
		err = yaml.Unmarshal(data, &inputdata)
		if err != nil {
			log.Printf("unable to unmarshal yaml with error %s\n", err)
		}
		err = desiredState.mergeInput(&inputdata)
		if err != nil {
			log.Fatalf("Failed to merge topic data: %s\n", err)
		}
	}

	return desiredState
}
