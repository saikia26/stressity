package loadtest

type KafkaConfig struct {
	Topic   string
	Brokers []string
	Enabled bool
}

type APIConfig struct {
	URL                       string
	Method                    string
	MaxIdleConnections        int
	MaxIdleConnectionsPerHost int
	RequestTimeOut            int64
	Enabled                   bool
}

type Conf struct {
	KafkaConfigs map[string]KafkaConfig
	APIConfigs   map[string]APIConfig
}
