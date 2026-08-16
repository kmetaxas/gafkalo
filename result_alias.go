package gafkalo

import (
	"github.com/kmetaxas/gafkalo/internal/result"
)

type Results = result.Results
type ClusterLinkResult = result.ClusterLinkResult
type TopicResult = result.TopicResult
type SchemaResult = result.SchemaResult
type ClientResult = result.ClientResult
type ConnectorResult = result.ConnectorResult
type ChangedConfig = result.ChangedConfig
type ChangedConnectorConfig = result.ChangedConnectorConfig
type ChangedClusterLinkConfig = result.ChangedClusterLinkConfig

func TopicResultFromTopic(topic Topic) TopicResult {
	return result.TopicResultFromTopic(topic)
}
