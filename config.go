package main

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/getsops/sops/v3"
	"github.com/getsops/sops/v3/decrypt"
	"github.com/kmetaxas/sarama-sasl/oauthbearer"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type KafkaConfig struct {
	Brokers []string `yaml:"bootstrapBrokers"`
	SSL     struct {
		Enabled    bool   `yaml:"enabled"`
		CA         string `yaml:"caPath"`
		SkipVerify bool   `yaml:"skipVerify"`
		ClientCert string `yaml:"clientCert"`
		ClientKey  string `yaml:"clientKey"`
	} `yaml:"ssl"`
	Krb5 struct {
		Enabled            bool   `yaml:"enabled"`
		Keytab             string `yaml:"keytab"`
		ServiceName        string `yaml:"serviceName"`
		Realm              string `yaml:"realm"`
		Username           string `yaml:"username"`
		Password           string `yaml:"password"`
		KerberosConfigPath string `yaml:"krb5Path"`
	} `yaml:"kerberos"`
	Producer struct {
		MaxMessageBytes int    `yaml:"maxMessageBytes"`
		Compression     string `yaml:"compression"`
	} `yaml:"producer"`
	Scram struct {
		Enabled  bool   `yaml:"enabled"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"scram"`
	TokenAuth struct {
		Enabled        bool   `yaml:"enabled"`
		ClientID       string `yaml:"client"`
		Secret         string `yaml:"secret"`
		TokenUrl       string `yaml:"url"`
		IsConfluentMDS bool   `yaml:"is_confluent_mds"`
		CA             string `yaml:"caPath"`
	} `yaml:"tokenauth"`
}
type MDSConfig struct {
	Url                     string `yaml:"url"`
	User                    string `yaml:"username"`
	Password                string `yaml:"password"`
	SchemaRegistryClusterID string `yaml:"schema-registry-cluster-id"`
	ConnectClusterId        string `yaml:"connect-cluster-id"`
	KSQLClusterID           string `yaml:"ksql-cluster-id"`
	CAPath                  string `yaml:"caPath"` // Add a trusted CA
	SkipVerify              bool   `yaml:"skipVerify"`
}

type ConnectConfig struct {
	Url        string `yaml:"url"`
	User       string `yaml:"username"`
	Password   string `yaml:"password"`
	CAPath     string `yaml:"caPath"` // Add a trusted CA
	SkipVerify bool   `yaml:"skipVerify"`
}
type SRConfig struct {
	Url        string        `yaml:"url"`
	Timeout    time.Duration `yaml:"timeout"` // Allow setting custom timeout for API calls
	Username   string        `yaml:"username"`
	Password   string        `yaml:"password"`
	CAPath     string        `yaml:"caPath"` // Add a trusted CA
	SkipVerify bool          `yaml:"skipVerify"`
	// When this is true, Gafkalo will read the _schemas topic directly and
	// use an internal cache for read operations, bypassign the REST API
	SkipRestForReads bool `yaml:"skipRegistryForReads"`
}
type Configuration struct {
	Connections struct {
		Kafka          KafkaConfig   `yaml:"kafka"`
		Schemaregistry SRConfig      `yaml:"schemaregistry"`
		Mds            MDSConfig     `yaml:"mds"`
		Connect        ConnectConfig `yaml:"connect"`
	} `yaml:"connections"`
	Kafkalo struct {
		InputDirs                    []string `yaml:"input_dirs"`
		SchemaDir                    string   `yaml:"schema_dir"` // Directory to look for schemas when using a relative path
		ConnectorsSensitiveKeysRegex string   `yaml:"connectors_sensitive_keys"`
	} `yaml:"kafkalo"`
}

func parseConfig(configFile string) Configuration {
	var configData, data []byte
	var err error
	data, err = os.ReadFile(configFile)
	if err != nil {
		log.Printf("unable to read %s with error %s\n", configFile, err)
	}
	configData, err = decrypt.Data(data, "yaml")
	/* try to decrypt using sops.
	If we have an error MetadataNotFound, then we consider the file plaintext and ignore this error
	*/
	if err != nil && err != sops.MetadataNotFound {
		log.Fatalf("Failed to read config: %s", err)
	} else if err == sops.MetadataNotFound {
		configData = data
	}

	var Config Configuration
	err = yaml.Unmarshal(configData, &Config)
	if err != nil {
		log.Fatalf("Failed to read kafkalo config: %s\n", err)
	}
	return Config
}

// Get the input patterns to use
func (conf *Configuration) GetInputPatterns() []string {
	return conf.Kafkalo.InputDirs
}

// Resolve input patterns to actual files to read (expand globs and list files)
func (conf *Configuration) ResolveFilesFromPatterns(patterns []string) ([]string, error) {
	var files []string
	for _, pattern := range patterns {
		// Check if we have a Glob pattern or a direct file
		if strings.Contains(pattern, "*") {
			// We have a glob pattern
			matches, err := filepath.Glob(pattern)
			// Filter out unwanted matches (like directories)
			for _, match := range matches {
				if isValidInputFile(match) {
					files = append(files, match)
				}
			}
			if err != nil {
				log.Fatalf("Could not read match pattern: %s\n", err)
			}
		} else {
			if isValidInputFile(pattern) {
				files = append(files, pattern)
			}
		}
	}
	return files, nil
}

// Takes a schema path as specified in the yaml and returns a "normalized" path
// If it is an absolute path, it is returned as is
// If it is a relative path it is made relative to the source dir
func normalizeSchemaPath(inputFile string) string {
	var res string
	if gafkaloConfig.Kafkalo.SchemaDir != "" {
		res = path.Join(gafkaloConfig.Kafkalo.SchemaDir, inputFile)
	} else {
		res = inputFile
	}
	return res
}

// Validates an input filename as valid (exists, is not dir etc)
func isValidInputFile(filename string) bool {
	fi, err := os.Stat(filename)
	if err != nil {
		// log.Printf("Ignoring file %s due to: %s\n", filename, err)
		return false
	}
	if !fi.Mode().IsRegular() {
		// log.Printf("Ignoring file %s because its not regular\n", filename)
		return false
	}
	return true
}

// Create a Sarama Config struct from a KafkaConfig struct
func SaramaConfigFromKafkaConfig(conf KafkaConfig) *sarama.Config {
	config := sarama.NewConfig()
	config.Metadata.Full = true
	config.Net.TLS.Enable = conf.SSL.Enabled
	if conf.Krb5.Enabled {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
		config.Net.SASL.GSSAPI.Realm = conf.Krb5.Realm
		config.Net.SASL.GSSAPI.Username = conf.Krb5.Username
		if conf.Krb5.Keytab != "" {
			config.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
			config.Net.SASL.GSSAPI.KeyTabPath = conf.Krb5.Keytab

		} else {
			config.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
			config.Net.SASL.GSSAPI.Password = conf.Krb5.Password
		}
		if conf.Krb5.ServiceName == "" {
			config.Net.SASL.GSSAPI.ServiceName = "kafka"
		} else {
			config.Net.SASL.GSSAPI.ServiceName = conf.Krb5.ServiceName
		}
		if conf.Krb5.KerberosConfigPath == "" {
			config.Net.SASL.GSSAPI.KerberosConfigPath = "/etc/krb5.conf"
		} else {
			config.Net.SASL.GSSAPI.KerberosConfigPath = conf.Krb5.KerberosConfigPath
		}
	}
	// Set Sasl plain if configured
	if conf.Scram.Enabled {

		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		config.Net.SASL.User = conf.Scram.Username
		config.Net.SASL.Password = conf.Scram.Password
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
	}
	// Token auth if configured
	if conf.TokenAuth.Enabled {
		if conf.TokenAuth.ClientID == "" {
			log.Fatal("required 'client' field  not defined in tokenauth section")
		}
		if conf.TokenAuth.Secret == "" {
			log.Fatal("required 'secret' field  not defined in tokenauth section")
		}
		if conf.TokenAuth.TokenUrl == "" {
			log.Fatal("required 'url' field  not defined in tokenauth section")
		}
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
		// We use a special handler if Confluent Metadata service tokens are used.
		if conf.TokenAuth.IsConfluentMDS {
			config.Net.SASL.TokenProvider = NewTokenProviderConfluentMDS(conf.TokenAuth.ClientID, conf.TokenAuth.Secret, conf.TokenAuth.TokenUrl, conf.TokenAuth.CA)
		} else {
			config.Net.SASL.TokenProvider = oauthbearer.NewTokenProvider(conf.TokenAuth.ClientID, conf.TokenAuth.Secret, conf.TokenAuth.TokenUrl)
		}
	}
	if conf.SSL.Enabled && (conf.SSL.CA != "" || conf.SSL.SkipVerify || conf.SSL.ClientCert != "" || conf.SSL.ClientKey != "") {
		tlsConfig := createTlsConfig(conf.SSL.CA, conf.SSL.SkipVerify)
		if conf.SSL.ClientCert != "" && conf.SSL.ClientKey != "" {
			tlsConfig = createMutualTlsConfig(tlsConfig, conf.SSL.ClientCert, conf.SSL.ClientKey)
		}
		config.Net.TLS.Config = tlsConfig
	}
	if conf.Producer.MaxMessageBytes != 0 {
		config.Producer.MaxMessageBytes = conf.Producer.MaxMessageBytes
	}
	if conf.Producer.Compression != "" {
		switch conf.Producer.Compression {
		case "snappy":
			config.Producer.Compression = sarama.CompressionSnappy
		case "gzip":
			config.Producer.Compression = sarama.CompressionGZIP
		case "lz4":
			config.Producer.Compression = sarama.CompressionLZ4
		case "zstd":
			config.Producer.Compression = sarama.CompressionZSTD
		case "none":
			config.Producer.Compression = sarama.CompressionNone
		}
	} else {
		// Use snappy as default if none other is specified
		config.Producer.Compression = sarama.CompressionSnappy
	}
	return config
}

func createTlsConfig(CAPath string, SkipVerify bool) *tls.Config {
	// Get system Cert Pool
	config := &tls.Config{}
	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		log.Fatal(err)
	}
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}
	if CAPath != "" {
		pem, err := os.ReadFile(CAPath)
		if err != nil {
			log.Fatal(err)
		}
		if ok := rootCAs.AppendCertsFromPEM(pem); !ok {
			log.Fatalf("Could not append cert %s to CertPool\n", CAPath)
		}
		log.Tracef("Created TLS Config from PEM %s (InsecureSkipVerify=%v)", pem, SkipVerify)
	}
	config.RootCAs = rootCAs
	config.InsecureSkipVerify = SkipVerify
	log.Tracef("Setting InsecureSkipVerify=%v", SkipVerify)
	return config
}

func createMutualTlsConfig(tlsConfig *tls.Config, clientCertPath, clientKeyPath string) *tls.Config {
	if clientCertPath == "" || clientKeyPath == "" {
		return tlsConfig
	}

	cert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		log.Fatalf("Failed to load client certificate and key: %s", err)
	}

	tlsConfig.Certificates = []tls.Certificate{cert}
	log.Tracef("Added client certificate from %s and key from %s", clientCertPath, clientKeyPath)
	return tlsConfig
}
