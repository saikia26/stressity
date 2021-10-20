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
	NumClients                int
	Enabled                   bool
}

type AppConf struct {
	KafkaConfigs map[string]KafkaConfig
	APIConfigs   map[string]APIConfig
}

type FeatureConf struct {
	Enabled           bool
	BatchSize         int
	BatchIntervalInMS int64
	RunDurationInSec  int64
	TotalCount        int
	APISchema         map[string]SchemaAttributes
	StreamSchema      map[string]SchemaAttributes
	KeyMeta           map[string]interface{}
}

type SchemaAttributes struct {
	Enabled    bool
	Definition map[string]interface{}
}
