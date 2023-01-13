package main

import (
	"reflect"
	"testing"
)

func TestParseOffsetsArg(t *testing.T) {
	var expected map[int32]int64
	var param string
	expected = make(map[int32]int64)
	param = "TOPIC=0:4,1:300,2:5124"
	res, err := parseOffsetsArg(&param)
	if err != nil {
		t.Error(err)
	}
	expected[0], expected[1], expected[2] = 4, 300, 5124
	if !reflect.DeepEqual(&res, &expected) {
		t.Errorf("%+vs not equal to %+vs", res, expected)
	}
	// now break it
	param = "TOPIC0:4,1:300,2:5124"
	res, err = parseOffsetsArg(&param)
	if err == nil {
		t.Error("Should get error about wrong structure")
	}
	param = "TOPIC=0:4,,1:300,2:5124"
	res, err = parseOffsetsArg(&param)
	if err == nil {
		t.Error("Should get error about wrong structure")
	}
}

// test LoadConfig()
func TestLoadConfig(t *testing.T) {
	config := LoadConfig("testdata/files/config.sample.yaml")
	if config.Connections.Schemaregistry.Username != "user" {
		t.Errorf("Schema registry username not 'username' (%s)", config.Connections.Schemaregistry.Username)
	}
	if config.Connections.Mds.Password != "password" {
		t.Errorf("MDS password not 'password' (%s)", config.Connections.Mds.Password)
	}

}

// Test GetInputData()
func TestGetInputData(t *testing.T) {
	config := LoadConfig("testdata/files/config.sample.yaml")
	inputdata := GetInputData(config)
	topic_count := len(inputdata.Topics)
	if topic_count != 9 {
		t.Errorf("Input topics <> 9 (%d)", topic_count)
	}
}

// Test GetAdminClients()
func TestGetAdminClients(t *testing.T) {
	config := LoadConfig("testdata/files/config.sample.yaml")
	k, _, _, _ := GetAdminClients(config)
	if k.DryRun == false {
		t.Error("dryrun should be true")
	}

}
