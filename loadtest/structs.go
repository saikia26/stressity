package loadtest

type KafkaConfig struct {
	Topic   string
	Brokers []string
	Enabled bool
}

type APIConfig struct {
	URL            string
	Method         string
	TimeoutInMS    int64
	Enabled        bool
	PipelineFactor int
	NumClients     int
}

type Conf struct {
	KafkaConfigs map[string]KafkaConfig
	APIConfigs   map[string]APIConfig
}
