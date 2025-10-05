package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMTLSAuthentication(t *testing.T) {
	ctx := context.Background()
	var extraMounts []string
	// Create temporary directory for certificates
	tempDir := createTempDir(t, "kafka-mtls-test")
	defer cleanUpTempDir(tempDir)

	kafkaContainer, sslPort, caPath, clientCertPath, clientKeyPath := generateKafkaContainerWithMTLS(t, ctx, tempDir, extraMounts)

	defer kafkaContainer.Terminate(ctx)

	// Test that mTLS configuration is properly created
	kafkaConfig := KafkaConfig{
		Brokers: []string{fmt.Sprintf("localhost:%s", sslPort.Port())},
		SSL: struct {
			Enabled    bool   `yaml:"enabled"`
			CA         string `yaml:"caPath"`
			SkipVerify bool   `yaml:"skipVerify"`
			ClientCert string `yaml:"clientCert"`
			ClientKey  string `yaml:"clientKey"`
		}{
			Enabled:    true,
			CA:         caPath,
			SkipVerify: false,
			ClientCert: clientCertPath,
			ClientKey:  clientKeyPath,
		},
	}

	// Test that the mTLS configuration is properly created
	saramaConfig := SaramaConfigFromKafkaConfig(kafkaConfig)
	assert.True(t, saramaConfig.Net.TLS.Enable)
	assert.NotNil(t, saramaConfig.Net.TLS.Config)
	assert.Len(t, saramaConfig.Net.TLS.Config.Certificates, 1)
	assert.NotNil(t, saramaConfig.Net.TLS.Config.RootCAs)

	// Verify the client certificate was loaded correctly
	clientCert := saramaConfig.Net.TLS.Config.Certificates[0]
	assert.NotEmpty(t, clientCert.Certificate)
	assert.NotNil(t, clientCert.PrivateKey)

	// Test actual SSL connection - this should now work with proper certificate setup
	client, err := sarama.NewClient(kafkaConfig.Brokers, saramaConfig)
	require.NoError(t, err, "SSL connection should succeed with proper certificate setup")
	defer client.Close()

	// Test basic operations over SSL
	topics, err := client.Topics()
	assert.NoError(t, err)
	assert.NotNil(t, topics)
	t.Logf("Successfully connected to Kafka over SSL and retrieved %d topics", len(topics))
}

func TestSCRAMAuthentication(t *testing.T) {
	ctx := context.Background()
	extraMounts := []string{}
	kafkaContainer, port := generateKafkaContainerWithSCRAM(t, ctx, extraMounts)

	defer kafkaContainer.Terminate(ctx)

	// Test that SCRAM configuration is properly created (without actual SASL connection)
	kafkaConfigScram := KafkaConfig{
		Brokers: []string{fmt.Sprintf("localhost:%s", port.Port())},
		SaslPlain: struct {
			Enabled  bool   `yaml:"enabled"`
			Username string `yaml:"username"`
			Password string `yaml:"password"`
		}{
			Enabled:  true,
			Username: "admin",
			Password: "admin-secret",
		},
	}

	saramaConfigScram := SaramaConfigFromKafkaConfig(kafkaConfigScram)
	client, err := sarama.NewClient(kafkaConfigScram.Brokers, saramaConfigScram)
	require.NoError(t, err)
	defer client.Close()

	// Test basic operations
	topics, err := client.Topics()
	assert.NoError(t, err)
	assert.NotNil(t, topics)
}

func TestTLSConfigCreation(t *testing.T) {
	// Test basic TLS config creation
	tlsConfig := createTlsConfig("", false)
	assert.NotNil(t, tlsConfig)
	assert.False(t, tlsConfig.InsecureSkipVerify)

	// Test TLS config with skip verify
	tlsConfigSkip := createTlsConfig("", true)
	assert.NotNil(t, tlsConfigSkip)
	assert.True(t, tlsConfigSkip.InsecureSkipVerify)
}

func TestMutualTLSConfigCreation(t *testing.T) {
	// Create temporary directory for certificates
	tempDir := createTempDir(t, "kafka-mtls-config-test")
	defer os.RemoveAll(tempDir)

	// Generate test certificates
	caPath, clientCertPath, clientKeyPath := generateTestCertificates(t, tempDir)

	// Test basic TLS config
	baseTlsConfig := createTlsConfig(caPath, false)
	assert.NotNil(t, baseTlsConfig)
	assert.NotNil(t, baseTlsConfig.RootCAs)

	// Test mutual TLS config creation
	mutualTlsConfig := createMutualTlsConfig(baseTlsConfig, clientCertPath, clientKeyPath)
	assert.NotNil(t, mutualTlsConfig)
	assert.Len(t, mutualTlsConfig.Certificates, 1)

	// Test with empty cert/key paths (should return original config)
	unchangedConfig := createMutualTlsConfig(baseTlsConfig, "", "")
	assert.Equal(t, baseTlsConfig, unchangedConfig)
}

func TestSaramaConfigFromKafkaConfig(t *testing.T) {
	// Test basic configuration
	kafkaConfig := KafkaConfig{
		Brokers: []string{"localhost:9092"},
	}

	saramaConfig := SaramaConfigFromKafkaConfig(kafkaConfig)
	assert.NotNil(t, saramaConfig)
	assert.True(t, saramaConfig.Metadata.Full)

	// Test SSL configuration
	kafkaConfigSSL := KafkaConfig{
		Brokers: []string{"localhost:9093"},
		SSL: struct {
			Enabled    bool   `yaml:"enabled"`
			CA         string `yaml:"caPath"`
			SkipVerify bool   `yaml:"skipVerify"`
			ClientCert string `yaml:"clientCert"`
			ClientKey  string `yaml:"clientKey"`
		}{
			Enabled:    true,
			SkipVerify: true,
		},
	}

	saramaConfigSSL := SaramaConfigFromKafkaConfig(kafkaConfigSSL)
	assert.True(t, saramaConfigSSL.Net.TLS.Enable)
	assert.NotNil(t, saramaConfigSSL.Net.TLS.Config)

	// Test SASL Plain configuration
	kafkaConfigSASL := KafkaConfig{
		Brokers: []string{"localhost:9094"},
		SaslPlain: struct {
			Enabled  bool   `yaml:"enabled"`
			Username string `yaml:"username"`
			Password string `yaml:"password"`
		}{
			Enabled:  true,
			Username: "testuser",
			Password: "testpass",
		},
	}

	saramaConfigSASL := SaramaConfigFromKafkaConfig(kafkaConfigSASL)
	assert.True(t, saramaConfigSASL.Net.SASL.Enable)
	assert.Equal(t, string(sarama.SASLTypePlaintext), string(saramaConfigSASL.Net.SASL.Mechanism))
	assert.Equal(t, "testuser", saramaConfigSASL.Net.SASL.User)
	assert.Equal(t, "testpass", saramaConfigSASL.Net.SASL.Password)
}
