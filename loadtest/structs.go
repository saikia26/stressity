package loadtest

type KafkaConfig struct {
	Topic    string
	Brokers  []string
	Enabled  bool
}

type Conf struct {
	KafkaConfigs map[string]KafkaConfig
}
