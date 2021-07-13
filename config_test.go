package main

import (
	"fmt"
	"github.com/google/go-cmp/cmp"
	"testing"
)

func TestParseConfig(t *testing.T) {
	var expectedConf Configuration
	expKafkaConf := KafkaConfig{Brokers: []string{"localhost:9093"}}
	expKafkaConf.SSL.Enabled = false
	expKafkaConf.SSL.CA = "/path/to/ca.crt"
	expKafkaConf.SSL.SkipVerify = false
	expKafkaConf.Krb5.Enabled = true
	expKafkaConf.Krb5.Keytab = "path_to.key"
	expKafkaConf.Krb5.ServiceName = "kafka"
	expKafkaConf.Krb5.Realm = "EXAMPLE.COM"
	expKafkaConf.Krb5.Username = "username"
	expKafkaConf.Krb5.Password = "password"
	expMDSConf := MDSConfig{Url: "http://localhost:8090", User: "username", Password: "password", SchemaRegistryClusterID: "schemaregistry", ConnectClusterId: "", KSQLClusterID: "", CAPath: "/home/arcanum/Downloads/ca.crt", SkipVerify: false}

	expSRConf := SRConfig{Url: "http://localhost:8081", Username: "user", Password: "password!", CAPath: "/home/arcanum/Downloads/ca.crt", SkipVerify: true}
	expectedConf.Connections.Kafka = expKafkaConf
	expectedConf.Connections.Mds = expMDSConf
	expectedConf.Connections.Schemaregistry = expSRConf
	expectedConf.Kafkalo.InputDirs = []string{"testdata/files/data/*", "testdata/files/data/team2.yaml"}
	expectedConf.Kafkalo.SchemaDir = "testdata/files/data/"

	config := parseConfig("testdata/files/config.sample.yaml")
	if config.Connections.Kafka.Brokers[0] != "localhost:9093" {
		t.Error("Broker list is wrong")
	}
	diff := cmp.Diff(config, expectedConf)
	if diff != "" {
		t.Errorf("Config differs: %s", diff)
	}
}

func TestGetInutPatterns(t *testing.T) {
	config := parseConfig("testdata/files/config.sample.yaml")
	inputPatterns := config.GetInputPatterns()
	if len(inputPatterns) != 2 {
		t.Error("Wrong number of inputPatterns returned")
	}

}
func TestResoveFilesFromPatterns(t *testing.T) {
	config := parseConfig("testdata/files/config.sample.yaml")
	patterns, err := config.ResolveFilesFromPatterns(config.GetInputPatterns())
	if err != nil {
		t.Error(err)
	}
	if len(patterns) != 1 || patterns[0] != "testdata/files/data/sample.yaml" {
		t.Error(fmt.Sprintf("ResolvedPatterns %+vs", patterns))
	}
}
func TestIsValidInputFile(t *testing.T) {
	if isValidInputFile("test") {
		t.Error("test is a Dir and should NOT be vaild")
	}
	if !isValidInputFile("testdata/files/config.sample.yaml") {
		t.Error("file testdata/files/config.sample.yaml should be valid")
	}

}
