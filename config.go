package main

import (
	"crypto/tls"
	"io/ioutil"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	krb5client "github.com/jcmturner/gokrb5/v8/client"
	krb5config "github.com/jcmturner/gokrb5/v8/config"
	krb5keytab "github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
	"go.mozilla.org/sops/v3"
	"go.mozilla.org/sops/v3/decrypt"
	"gopkg.in/yaml.v2"
)

type KafkaConfig struct {
	Brokers []string `yaml:"bootstrapBrokers"`
	SSL     struct {
		Enabled    bool   `yaml:"enabled"`
		CA         string `yaml:"caPath"`
		SkipVerify bool   `yaml:"skipVerify"`
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
	data, err = ioutil.ReadFile(configFile)
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
		//log.Printf("Ignoring file %s due to: %s\n", filename, err)
		return false
	}
	if !fi.Mode().IsRegular() {
		//log.Printf("Ignoring file %s because its not regular\n", filename)
		return false
	}
	return true
}
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
	if conf.SSL.Enabled && (conf.SSL.CA != "" || conf.SSL.SkipVerify) {
		tlsConfig := createTlsConfig(conf.SSL.CA, conf.SSL.SkipVerify)
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

// Create a new Franz-go client from the provided Gafkalo Config
func CreateFranzKafkaOptsFromKafkaConfig(conf KafkaConfig) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(conf.Brokers...),
		kgo.MaxVersions(kversion.V2_4_0()),
	}

	// If SSL is enabled, add a TLS dialer
	if conf.SSL.Enabled && (conf.SSL.CA != "" || conf.SSL.SkipVerify) {
		tlsConfig := createTlsConfig(conf.SSL.CA, conf.SSL.SkipVerify)
		tlsDialer := &tls.Dialer{
			NetDialer: &net.Dialer{Timeout: 30 * time.Second},
			Config:    tlsConfig,
		}
		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))

	}
	// If compression is specified, set it. otherwise we use the franz-go default which is snappy
	if conf.Producer.Compression != "" {
		switch conf.Producer.Compression {
		case "snappy":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
		case "gzip":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
		case "lz4":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
		case "zstd":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
		case "none":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
		}

	}
	// Do SASL
	if conf.Krb5.Enabled {
		saslConf := kerberos.Auth{
			Service: "kafka", // TODO should be configurable
		}

		krb5conf := Krb5GetConfig("/etc/krb5.conf")
		keytab := Krb5GetKeytab(conf.Krb5.Keytab)

		saslConf.Client = krb5client.NewWithKeytab(conf.Krb5.Username, conf.Krb5.Realm, keytab, krb5conf)
		// Login here, or does franz-go login for us?
		err := saslConf.Client.Login()
		if err != nil {
			log.Fatalf("Failed to login to kerberos with error %s", err)
		}
		opts = append(opts, kgo.SASL(saslConf.AsMechanism()))
	}
	/*


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
	*/
	// Add Logger
	// TODO match the global logger instead of a custom kgo specific logger..
	opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelDebug, nil)))
	return opts
}

func Krb5GetConfig(path string) *krb5config.Config {
	var krb5path string = "/etc/krb5.conf"
	if path != "" {
		krb5path = path
	}
	newConfig, err := krb5config.Load(krb5path)
	if err != nil {
		log.Fatalf("Unable to load krb5 conf from %s with error %s", path, err)
	}
	return newConfig

}

func Krb5GetKeytab(path string) *krb5keytab.Keytab {
	keytab, err := krb5keytab.Load(path)
	if err != nil {
		log.Fatalf("Unable to read keytab at %s with error %s", path, err)
	}
	return keytab
}
