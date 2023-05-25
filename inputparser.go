package main

import (
	"go.mozilla.org/sops/v3"
	"go.mozilla.org/sops/v3/decrypt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

// This represents the desired state that the user asked for
// It a merge of all individual input files
type DesiredState struct {
	Topics     map[string]Topic
	Clients    map[string]Client
	Connectors map[string]Connector
}

// This is the input Yaml file schema
type InputYaml struct {
	Topics     []Topic     `yaml:"topics"`
	Clients    []Client    `yaml:"clients"`
	Connectors []Connector `yaml:"connectors"`
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

	for _, connector := range data.Connectors {
		if _, exists := state.Connectors[connector.Name]; exists {
			log.Fatalf("Duplicate definition for connector %s", connector.Name)
		} else {
			state.Connectors[connector.Name] = connector
		}

	}
	return nil
}
func Parse(inputFiles []string) DesiredState {
	var desiredState = DesiredState{
		Topics:     make(map[string]Topic),
		Clients:    make(map[string]Client, 20),
		Connectors: make(map[string]Connector),
	}
	for _, filename := range inputFiles {
		log.Debugf("Processing YAML file %s", filename)
		rawData, err := ioutil.ReadFile(filename)
		if err != nil {
			log.Warnf("unable to read %s with error %s\n", filename, err)
		}
		// Try to decrypt using sops first, as some YAML may have sensitive info (for example connectors)
		data, err := decrypt.Data(rawData, "yaml")
		//	If we have an error MetadataNotFound, then we consider the file plaintext and ignore this error
		if err != nil && err != sops.MetadataNotFound {
			log.Fatalf("Failed to read config: %s", err)
		} else if err == sops.MetadataNotFound {
			data = rawData
		}

		var inputdata InputYaml
		err = yaml.Unmarshal(data, &inputdata)
		if err != nil {
			log.Fatalf("unable to unmarshal yaml with error %s\n", err)
		}
		err = desiredState.mergeInput(&inputdata)
		if err != nil {
			log.Fatalf("Failed to merge topic data: %s\n", err)
		}
	}

	return desiredState
}
