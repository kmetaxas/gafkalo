package main

type Client_topic_roles struct {
	Topic    string `yaml:"topic"`
	Prefixed bool   `yaml:"prefixed"`
}

type Client_group_roles struct {
	Name     string   `yaml:"name"`
	Roles    []string `yaml:"roles"`
	Prefixed bool     `yaml:"prefixed"`
}
type Client struct {
	Principal        string               `yaml:"principal"`
	ConsumerFor      []Client_topic_roles `yaml:"consumer_for"`
	ProducerFor      []Client_topic_roles `yaml:"producer_for"`
	ResourceownerFor []Client_topic_roles `yaml:"resourceowner_for"`
	Groups           []Client_group_roles `yaml:"groups"`
}
