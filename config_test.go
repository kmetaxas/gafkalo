package main

import (
	"fmt"
	"testing"
)

func TestParseConfig(t *testing.T) {
	// TODO instantiate structs with the expected data and compare them (possible cmp.Equal. Fow now we just verfiy a few things.
	config := parseConfig("test/files/config.sample.yaml")
	if config.Connections.Kafka.Brokers[0] != "localhost:9093" {
		t.Error("Broker list is wrong")
	}
	if !config.Connections.Kafka.Krb5.Enabled {
		t.Error("Krb should be enabled")
	}
	if config.Connections.Kafka.Krb5.Keytab != "path_to.key" {
		t.Error("Keytab not defined")
	}
	if config.Connections.Schemaregistry.Url != "http://localhost:8081" {
		t.Error("SR url could not be read")
	}

	if len(config.Kafkalo.InputDirs) != 2 {
		t.Error(fmt.Sprintf("Wrong number of InputDirs (found %d)", len(config.Kafkalo.InputDirs)))
	}

}

func TestGetInutPatterns(t *testing.T) {
	config := parseConfig("test/files/config.sample.yaml")
	inputPatterns := config.GetInputPatterns()
	if len(inputPatterns) != 2 {
		t.Error("Wrong number of inputPatterns returned")
	}

}
func TestResoveFilesFromPatterns(t *testing.T) {
	config := parseConfig("test/files/config.sample.yaml")
	patterns, err := config.ResolveFilesFromPatterns(config.GetInputPatterns())
	if err != nil {
		t.Error(err)
	}
	if len(patterns) != 1 || patterns[0] != "data/sample.yaml" {
		t.Error(fmt.Sprintf("GetInputPatterns returned %+vs", config.GetInputPatterns()))
	}
}
func TestIsValidInputFile(t *testing.T) {
	if isValidInputFile("test") {
		t.Error("test is a Dir and should NOT be vaild")
	}
	if !isValidInputFile("test/files/config.sample.yaml") {
		t.Error("file test/files/config.sample.yaml should be valid")
	}

}
