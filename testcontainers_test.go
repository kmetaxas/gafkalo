package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Helper function to create temporary directory, using RUNNER_TEMP if available (for GitHub Actions)
func createTempDir(t *testing.T, prefix string) string {
	var tempDir string
	var err error

	if runnerTemp := os.Getenv("RUNNER_TEMP"); runnerTemp != "" {
		return runnerTemp
	} else {
		tempDir, err = os.MkdirTemp("", prefix)
	}

	require.NoError(t, err)
	return tempDir
}

// Clean tempDir, but make sure we don't do that if we are using github actions runner temp
func cleanUpTempDir(tempDir string) {
	if runnerTemp := os.Getenv("RUNNER_TEMP"); runnerTemp != "" {
		return
	} else {
		os.RemoveAll(tempDir)
	}
}

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
	t.Logf("Script output: %s", output)
	if err != nil {
		t.Logf("Certificate creation output: %s", string(output))
		t.Skip("Skipping SSL test - requires openssl and keytool")
	}

	caPath := filepath.Join(tempDir, "ca.crt")
	clientCertPath := filepath.Join(tempDir, "client.crt")
	clientKeyPath := filepath.Join(tempDir, "client.key")

	return caPath, clientCertPath, clientKeyPath
}

func generateKafkaContainerWithMTLS(t *testing.T, ctx context.Context, tempDir string, extraMounts []string) (testcontainers.Container, nat.Port, string, string, string) {
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

	// Get the mapped SSL port
	sslPort, err := kafkaContainer.MappedPort(ctx, "9093")
	require.NoError(t, err)
	return kafkaContainer, sslPort, caPath, clientCertPath, clientKeyPath
}

func generateKafkaContainerWithSCRAM(t *testing.T, ctx context.Context, extraMounts []string) (testcontainers.Container, nat.Port) {
	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:latest",
		ExposedPorts: []string{"9092/tcp", "9093/tcp"},
		Env: map[string]string{
			"KAFKA_NODE_ID":                                                  "1",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":                           "CONTROLLER:PLAINTEXT,OUTSIDE:SASL_PLAINTEXT,INTERNAL:PLAINTEXT",
			"KAFKA_ADVERTISED_LISTENERS":                                     "OUTSIDE://localhost:9092,INTERNAL://localhost:9093",
			"KAFKA_PROCESS_ROLES":                                            "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                                 "1@localhost:29093",
			"KAFKA_LISTENERS":                                                "OUTSIDE://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093,INTERNAL://0.0.0.0:9093",
			"KAFKA_INTER_BROKER_LISTENER_NAME":                               "INTERNAL",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                                "CONTROLLER",
			"KAFKA_LOG_DIRS":                                                 "/tmp/kraft-combined-logs",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":                         "1",
			"CLUSTER_ID":                                                     "MkU3OEVBNTcwNTJENDM2Qk",
			"KAFKA_LISTENER_NAME_OUTSIDE_SASL_ENABLED_MECHANISMS":            "SCRAM-SHA-256",
			"KAFKA_SASL_ENABLED_MECHANISMS":                                  "SCRAM-SHA-256",
			"KAFKA_LISTENER_NAME_OUTSIDE_SCRAM___SHA___256_SASL_JAAS_CONFIG": "org.apache.kafka.common.security.scram.ScramLoginModule required;",
		},
		Cmd: []string{
			"bash",
			"-c",
			`
			. /etc/confluent/docker/bash-config
			
			echo "===> User"
			id
			
			echo "===> Configuring ..."
			/etc/confluent/docker/configure
			
			echo "===> Running preflight checks with SCRAM ... "
			export KAFKA_DATA_DIRS=${KAFKA_DATA_DIRS:-"/var/lib/kafka/data"}
			echo "===> Check if $KAFKA_DATA_DIRS is writable ..."
			dub path "$KAFKA_DATA_DIRS" writable
			
			echo "===> Using provided cluster id $CLUSTER_ID with SCRAM credentials..."
			result=$(kafka-storage format --cluster-id=$CLUSTER_ID -c /etc/kafka/kafka.properties --add-scram 'SCRAM-SHA-256=[name=admin,password=admin-secret]' 2>&1) || \
				echo $result | grep -i "already formatted" || \
				{ echo $result && (exit 1) }
			
			echo "===> Launching ... "
			exec /etc/confluent/docker/launch
			`,
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(60 * time.Second),
	}

	kafkaContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	// Get the mapped port
	mappedPort, err := kafkaContainer.MappedPort(ctx, "9092")
	require.NoError(t, err)
	return kafkaContainer, mappedPort
}
