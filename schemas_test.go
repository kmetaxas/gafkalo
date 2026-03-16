package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/kmetaxas/srclient"
	"github.com/stretchr/testify/require"
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

func TestSchemaRegistryCompatibilityCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, srContainer, _, srURL := generateKafkaAndSchemaRegistryContainers(t, ctx)
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Kafka container: %v", err)
		}
		if err := srContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Schema Registry container: %v", err)
		}
	}()

	config := &Configuration{}
	config.Connections.Schemaregistry.Url = srURL
	config.Connections.Schemaregistry.CheckCompatibility = true
	config.Connections.Schemaregistry.Timeout = 30

	admin := NewSRAdmin(config)

	initialSchema := Schema{
		SubjectName:   "test-compatibility-subject",
		Compatibility: "BACKWARD",
		SchemaData:    `{"type": "record", "name": "User", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}]}`,
		SchemaType:    "AVRO",
	}

	// Use ReconcileSchema to both register the schema AND set the compatibility level
	result := admin.ReconcileSchema(initialSchema, false)
	require.NotNil(t, result, "Failed to register initial schema")
	require.True(t, result.HasNewVersion(), "Should have registered initial schema")

	t.Run("Compatible schema change", func(t *testing.T) {
		compatibleSchema := Schema{
			SubjectName:   "test-compatibility-subject",
			Compatibility: "BACKWARD",
			SchemaData:    `{"type": "record", "name": "User", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}, {"name": "email", "type": ["null", "string"], "default": null}]}`,
			SchemaType:    "AVRO",
		}

		isCompatible, compatLevel, errors, err := admin.DoCompatibilityTest(compatibleSchema)
		require.NoError(t, err, "Compatibility check should not fail: %s", err)
		require.True(t, isCompatible, "Schema should be compatible (added optional field)")
		require.Equal(t, "BACKWARD", compatLevel, "Compatibility level should be BACKWARD")
		require.Empty(t, errors, "Should have no compatibility errors")
	})

	t.Run("Incompatible schema change", func(t *testing.T) {
		incompatibleSchema := Schema{
			SubjectName:   "test-compatibility-subject",
			Compatibility: "BACKWARD",
			SchemaData:    `{"type": "record", "name": "User", "fields": [{"name": "name", "type": "int"}]}`,
			SchemaType:    "AVRO",
		}

		isCompatible, compatLevel, errors, err := admin.DoCompatibilityTest(incompatibleSchema)
		require.NoError(t, err, "Compatibility check should not fail: %s", err)
		require.False(t, isCompatible, "Schema should be incompatible (incompatible type change)")
		require.Equal(t, "BACKWARD", compatLevel, "Compatibility level should be BACKWARD")
		require.NotEmpty(t, errors, "Should have compatibility error messages")

		t.Logf("Compatibility errors: %v", errors)
	})

	t.Run("ReconcileSchema with compatibility check enabled", func(t *testing.T) {
		newCompatibleSchema := Schema{
			SubjectName:   "test-reconcile-subject",
			Compatibility: "BACKWARD",
			SchemaData:    `{"type": "record", "name": "Product", "fields": [{"name": "id", "type": "string"}]}`,
			SchemaType:    "AVRO",
		}

		result := admin.ReconcileSchema(newCompatibleSchema, false)
		require.NotNil(t, result, "Result should not be nil")
		require.True(t, result.HasNewVersion(), "Should have new version (first registration)")
		require.False(t, result.HasCompatibilityCheck(), "Should not check compatibility for first version")
	})

	t.Run("ReconcileSchema prevents incompatible registration", func(t *testing.T) {
		baseSchema := Schema{
			SubjectName:   "test-prevent-incompatible",
			Compatibility: "BACKWARD",
			SchemaData:    `{"type": "record", "name": "Order", "fields": [{"name": "orderId", "type": "string"}, {"name": "amount", "type": "double"}]}`,
			SchemaType:    "AVRO",
		}

		result1 := admin.ReconcileSchema(baseSchema, false)
		require.NotNil(t, result1)
		require.True(t, result1.HasNewVersion(), "Should register initial schema")

		// Refresh the subject cache after first registration
		if admin.UseSRCache {
			admin.SubjectCache = admin.SRCache.GetSubjects()
		} else {
			subjects, err := admin.Client.GetSubjects()
			require.NoError(t, err)
			admin.SubjectCache = subjects
		}

		incompatibleSchema := Schema{
			SubjectName:   "test-prevent-incompatible",
			Compatibility: "BACKWARD",
			SchemaData:    `{"type": "record", "name": "Order", "fields": [{"name": "orderId", "type": "int"}]}`,
			SchemaType:    "AVRO",
		}

		result2 := admin.ReconcileSchema(incompatibleSchema, false)
		require.NotNil(t, result2)
		require.True(t, result2.HasCompatibilityCheck(), "Should have compatibility check")
		require.False(t, result2.IsSchemaCompatible(), "Should be incompatible")
		require.True(t, result2.HasCompatibilityErrors(), "Should have error messages")
		require.False(t, result2.HasNewVersion(), "Should NOT register incompatible schema")
	})
}

func TestSchemaRegistryCompatibilityCheckDisabled(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	kafkaContainer, srContainer, _, srURL := generateKafkaAndSchemaRegistryContainers(t, ctx)
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Kafka container: %v", err)
		}
		if err := srContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Schema Registry container: %v", err)
		}
	}()

	config := &Configuration{}
	config.Connections.Schemaregistry.Url = srURL
	config.Connections.Schemaregistry.CheckCompatibility = false
	config.Connections.Schemaregistry.Timeout = 30

	admin := NewSRAdmin(config)

	schema := Schema{
		SubjectName:   "test-no-compat-check",
		Compatibility: "BACKWARD",
		SchemaData:    `{"type": "record", "name": "Item", "fields": [{"name": "id", "type": "string"}]}`,
		SchemaType:    "AVRO",
	}

	result := admin.ReconcileSchema(schema, false)
	require.NotNil(t, result)
	require.True(t, result.HasNewVersion(), "Should register schema")
	require.False(t, result.HasCompatibilityCheck(), "Should NOT check compatibility when disabled")
}
