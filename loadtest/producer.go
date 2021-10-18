package loadtest

import (
	"errors"
	"strings"

	"github.com/Shopify/sarama"
)

var (
	producers = make(map[string]sarama.SyncProducer)
)

func InitProducers() error {
	errs := make([]string, 0, len(AppConfig.KafkaConfigs))
	for streamName, conf := range AppConfig.KafkaConfigs {
		if !conf.Enabled {
			continue
		}
		producer, err := initProducer(streamName, conf)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		producers[streamName] = producer
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ", "))
	}
	return nil
}

func initProducer(streamName string, conf KafkaConfig) (sarama.SyncProducer, error) {
	sarConf := sarama.NewConfig()
	sarConf.Producer.Return.Successes = true
	sarConf.Producer.RequiredAcks = sarama.WaitForAll
	sarConf.ClientID = streamName
	return sarama.NewSyncProducer(conf.Brokers, sarConf)
}

func stopAllProducers() {
	for _, producer := range producers {
		producer.Close()
	}
}

func publishForABatch(streamName string, msgs []*sarama.ProducerMessage) error {
	return producers[streamName].SendMessages(msgs)
}
