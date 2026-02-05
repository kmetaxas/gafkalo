package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
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

	kafkaConfigSCRAM := KafkaConfig{
		Brokers: []string{"localhost:9094"},
		Scram: SCRAMConfig{
			Enabled:   true,
			Mechanism: "SCRAM-SHA-256",
			Username:  "admin",
			Password:  "admin-secret",
		},
	}

	saramaConfig := SaramaConfigFromKafkaConfig(kafkaConfigSCRAM)
	client, err := sarama.NewClient([]string{fmt.Sprintf("localhost:%s", port.Port())}, saramaConfig)
	require.NoError(t, err)
	defer client.Close()

	// Test basic operations
	topics, err := client.Topics()
	assert.NoError(t, err)
	assert.NotNil(t, topics)
	t.Logf("Successfully connected to Kafka with SCRAM-SHA-256 and retrieved %d topics", len(topics))
}

func TestKerberos5Authentication(t *testing.T) {
	ctx := context.Background()
	tempDir := createTempDir(t, "kafka-krb5-test")
	defer cleanUpTempDir(tempDir)

	kafkaContainer, kdcContainer, port, clientKeytabPath, krb5ConfPath := generateKafkaContainerWithKrb5(t, ctx, tempDir)
	defer kafkaContainer.Terminate(ctx)
	defer kdcContainer.Terminate(ctx)

	// Verify keytab exists
	if _, err := os.Stat(clientKeytabPath); os.IsNotExist(err) {
		t.Fatalf("Client keytab not found at %s", clientKeytabPath)
	}
	t.Logf("Using client keytab: %s", clientKeytabPath)
	t.Logf("Using krb5.conf: %s", krb5ConfPath)

	// Verify files exist
	if _, err := os.Stat(clientKeytabPath); os.IsNotExist(err) {
		t.Fatalf("Client keytab does not exist: %s", clientKeytabPath)
	}
	if _, err := os.Stat(krb5ConfPath); os.IsNotExist(err) {
		t.Fatalf("krb5.conf does not exist: %s", krb5ConfPath)
	}
	t.Logf("Using client keytab: %s", clientKeytabPath)
	t.Logf("Using krb5.conf: %s", krb5ConfPath)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Net.SASL.Enable = true
	saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
	saramaConfig.Net.SASL.GSSAPI.ServiceName = "kafka"
	saramaConfig.Net.SASL.GSSAPI.Realm = "EXAMPLE.COM"
	saramaConfig.Net.SASL.GSSAPI.Username = "kafkaclient"
	saramaConfig.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
	saramaConfig.Net.SASL.GSSAPI.KeyTabPath = clientKeytabPath
	saramaConfig.Net.SASL.GSSAPI.KerberosConfigPath = krb5ConfPath
	saramaConfig.Net.SASL.GSSAPI.DisablePAFXFAST = true
	saramaConfig.Net.SASL.GSSAPI.CCachePath = "/tmp/krb5cc_sarama_123"
	saramaConfig.Metadata.Full = true
	sarama_logger := log.New()
	// saramaConfig.ApiVersionsRequest = false
	sarama_logger.SetReportCaller(true)
	// sarama_logger.SetLevel(log.TraceLevel)
	// sarama.Logger = sarama_logger

	client, err := sarama.NewClient([]string{fmt.Sprintf("localhost:%s", port.Port())}, saramaConfig)
	require.NoError(t, err)
	defer client.Close()

	topics, err := client.Topics()
	assert.NoError(t, err)
	assert.NotNil(t, topics)
	t.Logf("Successfully connected to Kafka with Kerberos and retrieved %d topics", len(topics))
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
		Scram: SCRAMConfig{
			Enabled:   true,
			Mechanism: "SCRAM-SHA-256",
			Username:  "testuser",
			Password:  "testpass",
		},
	}

	saramaConfigSASL := SaramaConfigFromKafkaConfig(kafkaConfigSASL)
	assert.True(t, saramaConfigSASL.Net.SASL.Enable)
	assert.Equal(t, string(sarama.SASLTypeSCRAMSHA256), string(saramaConfigSASL.Net.SASL.Mechanism))
	assert.Equal(t, "testuser", saramaConfigSASL.Net.SASL.User)
	assert.Equal(t, "testpass", saramaConfigSASL.Net.SASL.Password)
}
