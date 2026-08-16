package gafkalo

import (
	"crypto/tls"

	"github.com/IBM/sarama"
	internalconfig "github.com/kmetaxas/gafkalo/internal/config"
)

type SCRAMConfig = internalconfig.SCRAMConfig
type KafkaConfig = internalconfig.KafkaConfig
type MDSConfig = internalconfig.MDSConfig
type ConnectConfig = internalconfig.ConnectConfig
type RestProxyConfig = internalconfig.RestProxyConfig
type SRConfig = internalconfig.SRConfig
type Configuration = internalconfig.Configuration
type TokenProvider = internalconfig.TokenProvider
type XDGSCRAMClient = internalconfig.XDGSCRAMClient

var SHA256 = internalconfig.SHA256
var SHA512 = internalconfig.SHA512

func parseConfig(configFile string) Configuration {
	return internalconfig.ParseConfig(configFile)
}

func setCurrentConfig(conf Configuration) {
	internalconfig.SetCurrent(conf)
}

func normalizeSchemaPath(inputFile string) string {
	return internalconfig.NormalizeSchemaPath(inputFile)
}

func SaramaConfigFromKafkaConfig(conf KafkaConfig) *sarama.Config {
	return internalconfig.SaramaConfigFromKafkaConfig(conf)
}

func createTlsConfig(CAPath string, SkipVerify bool) *tls.Config {
	return internalconfig.CreateTLSConfig(CAPath, SkipVerify)
}

func NewTokenProviderConfluentMDS(client, secret, url, caPath string) sarama.AccessTokenProvider {
	return internalconfig.NewTokenProviderConfluentMDS(client, secret, url, caPath)
}
