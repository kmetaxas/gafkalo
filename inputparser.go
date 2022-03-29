package main

import (
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

// This represents the desired state that the user asked for
// It a merge of all individual input files
type DesiredState struct {
	Topics  map[string]Topic
	Clients map[string]Client
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
	// We don't do deduplication fore clients because its normal to have multiple definitions. We merge the rules in one big object for each principal
	for _, client := range data.Clients {
		if _, exists := state.Clients[client.Principal]; exists {
			state.Clients[client.Principal] = mergeClients(state.Clients[client.Principal], client)

		} else {
			state.Clients[client.Principal] = client
		}
	}
	return nil
}
func Parse(inputFiles []string) DesiredState {
	var desiredState = DesiredState{Topics: make(map[string]Topic), Clients: make(map[string]Client, 20)}
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
