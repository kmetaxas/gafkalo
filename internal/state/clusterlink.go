package state

type ClusterLinkConfigDiff struct {
	Name           string                              `json:"name" yaml:"name"`
	ChangedConfigs map[string]ClusterLinkChangedConfig `json:"changed_configs"`
}

type ClusterLinkChangedConfig struct {
	OldValue *string `json:"old" yaml:"old"`
	NewValue *string `json:"new" yaml:"new"`
}
