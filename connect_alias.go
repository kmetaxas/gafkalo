package gafkalo

import internalconnect "github.com/kmetaxas/gafkalo/internal/connect"

type ConnectAdmin = internalconnect.ConnectAdmin
type ConnectClusterState = internalconnect.ConnectClusterState
type TaskStatus = internalconnect.TaskStatus
type Task = internalconnect.Task
type Connector = internalconnect.Connector
type ConnectorStatus = internalconnect.ConnectorStatus
type ConnectorPlugin = internalconnect.ConnectorPlugin
type ConnectorPluginValidateResponseConfig = internalconnect.ConnectorPluginValidateResponseConfig
type ConnectorPluginValidateResponse = internalconnect.ConnectorPluginValidateResponse

func NewConnectAdmin(config *ConnectConfig) (*ConnectAdmin, error) {
	return internalconnect.NewConnectAdmin(config)
}

func NewConnectorFromJson(jsonDefinition string) (*Connector, error) {
	return internalconnect.NewConnectorFromJson(jsonDefinition)
}
