package main

import (
	"testing"
)

func TestIsSensitiveField(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected bool
	}{
		{"empty string", "", false},
		{"single asterisk", "*", true},
		{"multiple asterisks", "********", true},
		{"mixed characters", "***abc***", false},
		{"regular value", "mypassword", false},
		{"asterisk with space", "*** ***", false},
		{"unicode dot that we also get", "••••••••••••", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSensitiveField(tt.value)
			if result != tt.expected {
				t.Errorf("isSensitiveField(%q) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}

func TestDetectSensitiveFields(t *testing.T) {
	connector := Connector{
		Name:  "trololo",
		Tasks: []Task{},
		Config: map[string]string{
			"connector.class":     "io.confluent.connect.jdbc.JdbcSourceConnector",
			"tasks.max":           "1",
			"connection.url":      "jdbc:postgresql://localhost/db",
			"connection.user":     "admin",
			"connection.password": "********",
			"api.key":             "****************",
			"normal.config":       "normalvalue",
		},
	}

	sensitiveFields := connector.detectSensitiveFields()

	if len(sensitiveFields) != 2 {
		t.Errorf("Expected 2 sensitive fields, got %d", len(sensitiveFields))
	}

	if !sensitiveFields["connection.password"] {
		t.Error("Expected connection.password to be detected as sensitive")
	}

	if !sensitiveFields["api.key"] {
		t.Error("Expected api.key to be detected as sensitive")
	}

	if sensitiveFields["connector.class"] {
		t.Error("connector.class should not be detected as sensitive")
	}

	if sensitiveFields["normal.config"] {
		t.Error("normal.config should not be detected as sensitive")
	}
}
