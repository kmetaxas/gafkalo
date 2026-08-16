package state

type ClientTopicRole struct {
	Topic      string `yaml:"topic"`
	IsLiteral  bool   `yaml:"isLiteral"`
	Strict     bool   `yaml:"strict"`
	Idempotent bool   `yaml:"idempotent"`
}

type ClientGroupRole struct {
	Name      string   `yaml:"name"`
	Roles     []string `yaml:"roles"`
	IsLiteral bool     `yaml:"isLiteral" default:"true"`
}

type ClientTransactionalIdRole struct {
	Name      string   `yaml:"name"`
	Roles     []string `yaml:"roles"`
	IsLiteral bool     `yaml:"isLiteral" default:"true"`
}

type Client struct {
	Principal        string                      `yaml:"principal"`
	ConsumerFor      []ClientTopicRole           `yaml:"consumer_for"`
	ProducerFor      []ClientTopicRole           `yaml:"producer_for"`
	ResourceownerFor []ClientTopicRole           `yaml:"resourceowner_for"`
	Groups           []ClientGroupRole           `yaml:"groups"`
	TransactionalIds []ClientTransactionalIdRole `yaml:"transactional_ids"`
}

func MergeClients(target Client, source Client) Client {
	target.ConsumerFor = append(target.ConsumerFor, source.ConsumerFor...)
	target.ProducerFor = append(target.ProducerFor, source.ProducerFor...)
	target.ResourceownerFor = append(target.ResourceownerFor, source.ResourceownerFor...)
	target.Groups = append(target.Groups, source.Groups...)
	return target
}
