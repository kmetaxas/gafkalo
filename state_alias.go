package gafkalo

import (
	"github.com/kmetaxas/gafkalo/internal/state"
	"github.com/kmetaxas/srclient"
)

type Schema = state.Schema
type Topic = state.Topic
type TopicPlan = state.TopicPlan
type ClientTopicRole = state.ClientTopicRole
type ClientGroupRole = state.ClientGroupRole
type ClientTransactionalIdRole = state.ClientTransactionalIdRole
type Client = state.Client
type ClusterLinkConfigDiff = state.ClusterLinkConfigDiff
type ClusterLinkChangedConfig = state.ClusterLinkChangedConfig

func CreateSchema(SubjectName string, SchemaPath string, Compatibility string, SchemaType srclient.SchemaType) (Schema, error) {
	return state.CreateSchema(SubjectName, SchemaPath, Compatibility, SchemaType)
}

func mergeClients(target Client, source Client) Client {
	return state.MergeClients(target, source)
}
