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
	TimeoutInMS               int64
	Enabled                   bool
	NumClients                int
}

type Conf struct {
	KafkaConfigs map[string]KafkaConfig
	APIConfigs   map[string]APIConfig
}
