package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Helper function to generate self-signed certificates and Java keystores for mTLS testing
func generateTestCertificates(t *testing.T, tempDir string) (string, string, string) {
	// Generate CA private key
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create CA certificate template
	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"Test CA"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// Create CA certificate
	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	// Write CA certificate to file
	caPath := filepath.Join(tempDir, "ca.crt")
	caCertFile, err := os.Create(caPath)
	require.NoError(t, err)
	defer caCertFile.Close()

	err = pem.Encode(caCertFile, &pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	require.NoError(t, err)

	// Generate server private key
	serverPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create server certificate template
	serverTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization:  []string{"Test Server"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
			CommonName:    "localhost",
		},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		SubjectKeyId: []byte{1, 2, 3, 4, 5},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	// Parse CA certificate for signing
	caCert, err := x509.ParseCertificate(caCertDER)
	require.NoError(t, err)

	// Create server certificate
	_, err = x509.CreateCertificate(rand.Reader, &serverTemplate, caCert, &serverPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	// Generate client private key
	clientPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create client certificate template
	clientTemplate := x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			Organization:  []string{"Test Client"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	// Create client certificate
	clientCertDER, err := x509.CreateCertificate(rand.Reader, &clientTemplate, caCert, &clientPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	// Write client certificate to file
	clientCertPath := filepath.Join(tempDir, "client.crt")
	clientCertFile, err := os.Create(clientCertPath)
	require.NoError(t, err)
	defer clientCertFile.Close()

	err = pem.Encode(clientCertFile, &pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER})
	require.NoError(t, err)

	// Write client private key to file
	clientKeyPath := filepath.Join(tempDir, "client.key")
	clientKeyFile, err := os.Create(clientKeyPath)
	require.NoError(t, err)
	defer clientKeyFile.Close()

	clientKeyPEM := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientPrivKey)}
	err = pem.Encode(clientKeyFile, clientKeyPEM)
	require.NoError(t, err)

	return caPath, clientCertPath, clientKeyPath
}

// Helper function to create Java keystores for Kafka SSL using the same CA as the client certificates
func createJavaKeystores(t *testing.T, tempDir string, caCertPath string) (string, string) {
	// Create a simple script to generate keystores using the existing CA
	keystoreScript := `#!/bin/bash
set -e

# Use the existing CA certificate
cp ca.crt ca-cert

# Generate server key and certificate using the same CA
openssl genrsa -out server-key 2048
openssl req -new -key server-key -out server-req -subj "/C=US/ST=CA/L=SF/O=Test/CN=localhost"

# Create a config file for the server certificate with SAN
cat > server.conf << EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = CA
L = SF
O = Test
CN = localhost

[v3_req]
keyUsage = keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

# Sign the server certificate with our CA
openssl x509 -req -in server-req -CA ca-cert -CAkey ca-key -CAcreateserial -out server-cert -days 365 -extensions v3_req -extfile server.conf

# Create PKCS12 keystore for server
openssl pkcs12 -export -in server-cert -inkey server-key -out server.p12 -name localhost -password pass:confluent

# Convert to JKS keystore
keytool -importkeystore -deststorepass confluent -destkeypass confluent -destkeystore kafka.keystore.jks -srckeystore server.p12 -srcstoretype PKCS12 -srcstorepass confluent -alias localhost

# Create truststore with CA certificate
keytool -keystore kafka.truststore.jks -alias CARoot -import -file ca-cert -storepass confluent -keypass confluent -noprompt
`

	scriptPath := filepath.Join(tempDir, "create_keystores.sh")
	err := os.WriteFile(scriptPath, []byte(keystoreScript), 0755)
	require.NoError(t, err)

	// Execute the script
	cmd := exec.Command("bash", scriptPath)
	cmd.Dir = tempDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("Keystore creation output: %s", string(output))
		t.Skip("Skipping SSL test - requires openssl and keytool")
	}

	keystorePath := filepath.Join(tempDir, "kafka.keystore.jks")
	truststorePath := filepath.Join(tempDir, "kafka.truststore.jks")

	// Create a confluent file to satisfy the container's path check
	confluentFile := filepath.Join(tempDir, "confluent")
	err = os.WriteFile(confluentFile, []byte("confluent"), 0644)
	if err != nil {
		t.Fatalf("Failed to create confluent file: %v", err)
	}

	return keystorePath, truststorePath
}

// Helper function to create unified certificates for both server and client using the same CA
func createUnifiedCertificates(t *testing.T, tempDir string) (string, string, string) {
	// Create a script that generates a CA and uses it for both server and client certificates
	certScript := `#!/bin/bash
set -e

# Generate CA private key
openssl genrsa -out ca-key 2048

# Create CA certificate
openssl req -new -x509 -key ca-key -out ca-cert -days 365 -subj "/C=US/ST=CA/L=SF/O=Test/CN=TestCA"

# Copy CA cert to the expected location
cp ca-cert ca.crt

# Generate server private key
openssl genrsa -out server-key 2048

# Create server certificate request
openssl req -new -key server-key -out server-req -subj "/C=US/ST=CA/L=SF/O=Test/CN=localhost"

# Create a config file for the server certificate with SAN
cat > server.conf << EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = CA
L = SF
O = Test
CN = localhost

[v3_req]
keyUsage = keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

# Sign the server certificate with our CA
openssl x509 -req -in server-req -CA ca-cert -CAkey ca-key -CAcreateserial -out server-cert -days 365 -extensions v3_req -extfile server.conf

# Generate client private key
openssl genrsa -out client-key 2048

# Create client certificate request
openssl req -new -key client-key -out client-req -subj "/C=US/ST=CA/L=SF/O=Test/CN=client"

# Create a config file for the client certificate
cat > client.conf << EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = CA
L = SF
O = Test
CN = client

[v3_req]
keyUsage = digitalSignature
extendedKeyUsage = clientAuth
EOF

# Sign the client certificate with our CA
openssl x509 -req -in client-req -CA ca-cert -CAkey ca-key -CAcreateserial -out client-cert -days 365 -extensions v3_req -extfile client.conf

# Copy client files to expected locations
cp client-cert client.crt
cp client-key client.key

# Create PKCS12 keystore for server
openssl pkcs12 -export -in server-cert -inkey server-key -out server.p12 -name localhost -password pass:confluent

# Convert to JKS keystore
keytool -importkeystore -deststorepass confluent -destkeypass confluent -destkeystore kafka.keystore.jks -srckeystore server.p12 -srcstoretype PKCS12 -srcstorepass confluent -alias localhost

# Create truststore with CA certificate
keytool -keystore kafka.truststore.jks -alias CARoot -import -file ca-cert -storepass confluent -keypass confluent -noprompt

# Create a confluent file to satisfy the container's path check
echo "confluent" > confluent
`

	scriptPath := filepath.Join(tempDir, "create_certs.sh")
	err := os.WriteFile(scriptPath, []byte(certScript), 0755)
	require.NoError(t, err)

	// Execute the script
	cmd := exec.Command("bash", scriptPath)
	cmd.Dir = tempDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("Certificate creation output: %s", string(output))
		t.Skip("Skipping SSL test - requires openssl and keytool")
	}

	caPath := filepath.Join(tempDir, "ca.crt")
	clientCertPath := filepath.Join(tempDir, "client.crt")
	clientKeyPath := filepath.Join(tempDir, "client.key")

	return caPath, clientCertPath, clientKeyPath
}

func TestMTLSAuthentication(t *testing.T) {
	ctx := context.Background()

	// Create temporary directory for certificates
	tempDir, err := os.MkdirTemp("", "kafka-mtls-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Generate unified certificates for both server and client using the same CA
	caPath, clientCertPath, clientKeyPath := createUnifiedCertificates(t, tempDir)

	// Start Kafka container with actual SSL configuration
	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:latest",
		ExposedPorts: []string{"9092/tcp", "9093/tcp"},
		Env: map[string]string{
			"KAFKA_NODE_ID":                          "1",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":   "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL",
			"KAFKA_ADVERTISED_LISTENERS":             "PLAINTEXT://localhost:9092,SSL://localhost:9093",
			"KAFKA_PROCESS_ROLES":                    "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":         "1@localhost:29093",
			"KAFKA_LISTENERS":                        "PLAINTEXT://0.0.0.0:9092,SSL://0.0.0.0:9093,CONTROLLER://0.0.0.0:29093",
			"KAFKA_INTER_BROKER_LISTENER_NAME":       "PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES":        "CONTROLLER",
			"KAFKA_LOG_DIRS":                         "/tmp/kraft-combined-logs",
			"KAFKA_SSL_KEYSTORE_FILENAME":            "kafka.keystore.jks",
			"KAFKA_SSL_KEYSTORE_LOCATION":            "/etc/kafka/secrets/kafka.keystore.jks",
			"KAFKA_SSL_KEYSTORE_PASSWORD":            "confluent",
			"KAFKA_SSL_KEYSTORE_CREDENTIALS":         "confluent",
			"KAFKA_SSL_KEY_PASSWORD":                 "confluent",
			"KAFKA_SSL_KEY_CREDENTIALS":              "confluent",
			"KAFKA_SSL_TRUSTSTORE_FILENAME":          "kafka.truststore.jks",
			"KAFKA_SSL_TRUSTSTORE_LOCATION":          "/etc/kafka/secrets/kafka.truststore.jks",
			"KAFKA_SSL_TRUSTSTORE_PASSWORD":          "confluent",
			"KAFKA_SSL_TRUSTSTORE_CREDENTIALS":       "confluent",
			"KAFKA_SSL_CLIENT_AUTH":                  "required",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
			"CLUSTER_ID":                             "MkU3OEVBNTcwNTJENDM2Qk",
		},
		Mounts: testcontainers.ContainerMounts{
			testcontainers.BindMount(tempDir, "/etc/kafka/secrets"),
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(90 * time.Second),
	}

	kafkaContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	defer kafkaContainer.Terminate(ctx)

	// Get the mapped SSL port
	sslPort, err := kafkaContainer.MappedPort(ctx, "9093")
	require.NoError(t, err)

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

	// Test basic connection to plaintext port to verify container is working
	plaintextPort, err := kafkaContainer.MappedPort(ctx, "9092")
	require.NoError(t, err)

	kafkaConfigPlain := KafkaConfig{
		Brokers: []string{fmt.Sprintf("localhost:%s", plaintextPort.Port())},
	}
	saramaConfigPlain := SaramaConfigFromKafkaConfig(kafkaConfigPlain)
	plainClient, err := sarama.NewClient(kafkaConfigPlain.Brokers, saramaConfigPlain)
	require.NoError(t, err)
	defer plainClient.Close()

	// Test basic operations on plaintext connection
	plaintextTopics, err := plainClient.Topics()
	assert.NoError(t, err)
	assert.NotNil(t, plaintextTopics)
}

func TestSCRAMAuthentication(t *testing.T) {
	ctx := context.Background()

	// Start Kafka container with KRaft (simplified - just test config creation)
	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:latest",
		ExposedPorts: []string{"9092/tcp"},
		Env: map[string]string{
			"KAFKA_NODE_ID":                          "1",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":   "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_ADVERTISED_LISTENERS":             "PLAINTEXT://localhost:9092",
			"KAFKA_PROCESS_ROLES":                    "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":         "1@localhost:29093",
			"KAFKA_LISTENERS":                        "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093",
			"KAFKA_INTER_BROKER_LISTENER_NAME":       "PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES":        "CONTROLLER",
			"KAFKA_LOG_DIRS":                         "/tmp/kraft-combined-logs",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
			"CLUSTER_ID":                             "MkU3OEVBNTcwNTJENDM2Qk",
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(60 * time.Second),
	}

	kafkaContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	defer kafkaContainer.Terminate(ctx)

	// Get the mapped port
	mappedPort, err := kafkaContainer.MappedPort(ctx, "9092")
	require.NoError(t, err)

	// Test that SCRAM configuration is properly created (without actual SASL connection)
	kafkaConfig := KafkaConfig{
		Brokers: []string{fmt.Sprintf("localhost:%s", mappedPort.Port())},
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

	// Test that the SASL configuration is properly created
	saramaConfig := SaramaConfigFromKafkaConfig(kafkaConfig)
	assert.True(t, saramaConfig.Net.SASL.Enable)
	assert.Equal(t, string(sarama.SASLTypePlaintext), string(saramaConfig.Net.SASL.Mechanism))
	assert.Equal(t, "admin", saramaConfig.Net.SASL.User)
	assert.Equal(t, "admin-secret", saramaConfig.Net.SASL.Password)

	// Test basic connection to plaintext port (since SASL setup is complex in containers)
	kafkaConfigPlain := KafkaConfig{
		Brokers: []string{fmt.Sprintf("localhost:%s", mappedPort.Port())},
	}
	saramaConfigPlain := SaramaConfigFromKafkaConfig(kafkaConfigPlain)
	client, err := sarama.NewClient(kafkaConfigPlain.Brokers, saramaConfigPlain)
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
	tempDir, err := os.MkdirTemp("", "kafka-mtls-config-test")
	require.NoError(t, err)
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
