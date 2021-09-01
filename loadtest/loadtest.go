package loadtest

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
	"time"
)

const (
	keyType      = "type"
	keyRawVal    = "rawVal"
	keyMeta      = "meta"
	keyObject    = "object"
	keyObjectMap = "objectMap"
)

type StreamSchema struct {
	StreamName        string
	Enabled           bool
	BatchSize         int
	BatchIntervalInMS int64
	RunDurationInSec  int64
	TotalCount        int
	Schema            map[string]interface{}
}

var (
	AppConfig Conf
	Schemas   map[string]StreamSchema
)

func StartLoadTest() {
	wg := &sync.WaitGroup{}
	for schemaName, schemaConf := range Schemas {
		if !schemaConf.Enabled {
			continue
		}
		wg.Add(1)
		go loadTestSchema(schemaName, schemaConf, wg)
	}

	wg.Wait()
	fmt.Printf("\n\nDone!\n\n")
}

func loadTestSchema(schemaName string, schemaConf StreamSchema, wg *sync.WaitGroup) {
	startTime := time.Now()
	currCount := 0
	batchCount := 0
	defer func() {
		fmt.Printf("\n\nLoad test for %s completed!\nTotal count: %d\nStart time: %s\nEnd time: %s\n", schemaName, currCount, startTime.String(), time.Now().String())
		wg.Done()
	}()

	endTime := startTime.Add(time.Duration(schemaConf.RunDurationInSec) * time.Second)
	maxCount := schemaConf.TotalCount
	batchSize := schemaConf.BatchSize
	sleepDuration := time.Duration(schemaConf.BatchIntervalInMS) * time.Millisecond

	for currCount < maxCount && time.Now().Before(endTime) {
		batchCount++
		fmt.Printf("\nProcessing %s batch %d | Total done till now - %d", schemaName, batchCount, currCount)
		nextBatchSize := getNextBatchSize(currCount, maxCount, batchSize)
		producerMsgs := getNextDataBatch(nextBatchSize, schemaName, schemaConf.Schema, AppConfig.KafkaConfigs[schemaConf.StreamName].Topic)
		err := publish(schemaConf.StreamName, producerMsgs)
		if err != nil {
			fmt.Printf("\nError while publishing batch %d of %s, err: %v, retying...", batchCount, schemaName, err)
		} else {
			currCount += len(producerMsgs)
		}
		time.Sleep(sleepDuration)
	}
}

func getNextBatchSize(countDone, maxCount, batchSize int) int {
	currBatchSize := batchSize
	if countDone+batchSize > maxCount {
		currBatchSize = maxCount - countDone
	}
	return currBatchSize
}

func getNextDataBatch(batchSize int, schemaName string, schema map[string]interface{}, topic string) []*sarama.ProducerMessage {
	msgsToPublish := make([]*sarama.ProducerMessage, 0, batchSize)
	for count := 0; count < batchSize; count++ {
		data := getDataFromSchema(schema)
		msg, err := json.Marshal(data)
		if err != nil {
			fmt.Printf("\nError while unmarshaling data for %s, err: %v", schemaName, err)
			continue
		}
		msgsToPublish = append(msgsToPublish, &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(msg),
		})
	}
	return msgsToPublish
}

func getDataFromSchema(schema map[string]interface{}) map[string]interface{} {
	res := make(map[string]interface{})
	for key, valObj := range schema {
		valMap, _ := valObj.(map[string]interface{})
		if rawVal, ok := valMap[keyRawVal]; ok {
			res[key] = rawVal
			continue
		}
		if valMap[keyType] == keyObject {
			res[key] = getDataFromSchema(valMap[keyObjectMap].(map[string]interface{}))
			continue
		}
		res[key] = valueFinders[valMap[keyType].(string)](valMap[keyMeta].(map[string]interface{}))
	}
	return res
}
