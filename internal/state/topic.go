package state

type Topic struct {
	Name              string             `yaml:"name"`
	Partitions        int32              `yaml:"partitions"`
	ReplicationFactor int16              `yaml:"replication_factor"`
	Configs           map[string]*string `yaml:"configs"`
	Key               Schema             `yaml:"key"`
	Value             Schema             `yaml:"value"`
}

type TopicPlan struct {
	Topic       string
	Reason      string
	ConfigDelta []struct {
		Key    string
		Before string
		After  string
	}
}

func (s *Topic) UnmarshalYAML(unmarshal func(any) error) error {
	type rawTopic Topic
	raw := rawTopic{}
	if err := unmarshal(&raw); err != nil {
		return err
	}
	if (Schema{} != raw.Key) {
		raw.Key.SubjectName = raw.Name + "-key"
	}
	if (Schema{} != raw.Value) {
		raw.Value.SubjectName = raw.Name + "-value"
	}

	*s = Topic(raw)
	return nil
}
