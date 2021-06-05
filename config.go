package main

import (
	//	"fmt"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
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

type SRConfig struct {
	Url        string `yaml:"url"`
	Username   string `yaml:"username"`
	Password   string `yaml:"password"`
	CAPath     string `yaml:"caPath"` // Add a trusted CA
	SkipVerify bool   `yaml:"skipVerify"`
}
type Configuration struct {
	Connections struct {
		Kafka          KafkaConfig `yaml:"kafka"`
		Schemaregistry SRConfig    `yaml:"schemaregistry"`
		Mds            MDSConfig   `yaml:"mds"`
	} `yaml:"connections"`
	Kafkalo struct {
		InputDirs []string `yaml:"input_dirs"`
	} `yaml:"kafkalo"`
}

func parseConfig(configFile string) Configuration {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Printf("unable to read %s with error %s\n", configFile, err)
	}

	var Config Configuration
	err = yaml.Unmarshal(data, &Config)
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
	fmt.Printf("Resolve returning files: %s\n", files)
	return files, nil
}

// Validates an input filename as valid (exists, is not dir etc)
func isValidInputFile(filename string) bool {
	fi, err := os.Stat(filename)
	if err != nil {
		log.Printf("Ignoring file %s due to: %s\n", filename, err)
		return false
	}
	if !fi.Mode().IsRegular() {
		log.Printf("Ignoring file %s because its not regular\n", filename)
		return false
	}
	return true
}
