package main

import (
	"fmt"
	"testing"

	"github.com/kmetaxas/srclient"
)

func TestGetSubjectForTopic(t *testing.T) {

	topic := "TOPIC1"
	expected_value := fmt.Sprintf("%s-value", topic)
	expected_key := fmt.Sprintf("%s-key", topic)

	test1 := getSubjectForTopic(topic, false)
	if test1 != expected_value {
		t.Errorf("%s != %s", test1, expected_value)
	}
	test2 := getSubjectForTopic(topic, true)
	if test2 != expected_key {
		t.Errorf("%s != %s", test2, expected_key)
	}

}

func TestCreateSchemaFields(t *testing.T) {
	schema, err := CreateSchema("test-subject", "", "BACKWARD", srclient.Avro)
	if err != nil {
		t.Errorf("Failed to create schema: %v", err)
	}
	if schema.SubjectName != "test-subject" {
		t.Errorf("Subject name mismatch: got %s, want test-subject", schema.SubjectName)
	}
	if schema.Compatibility != "BACKWARD" {
		t.Errorf("Compatibility mismatch: got %s, want BACKWARD", schema.Compatibility)
	}
}

func TestCreateSchemaWithDefaultType(t *testing.T) {
	schema, err := CreateSchema("test-subject", "", "", "")
	if err != nil {
		t.Errorf("Failed to create schema: %v", err)
	}
	if schema.SchemaType != "" {
		t.Errorf("Schema type should be empty when no data provided, got %s", schema.SchemaType)
	}
}

func getTestSchema() Schema {
	return Schema{
		SubjectName:   "test-subject",
		SchemaPath:    "",
		Compatibility: "BACKWARD",
		SchemaData:    `{"type": "record", "name": "test", "fields": [{"name": "field1", "type": "string"}]}`,
		SchemaType:    srclient.Avro,
	}
}
