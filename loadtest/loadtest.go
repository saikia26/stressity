package loadtest

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

const (
	keyType      = "type"
	keyRawVal    = "rawVal"
	keyMeta      = "meta"
	keyObject    = "object"
	keyObjectMap = "objectMap"
)

type Schema struct {
	Enabled           bool
	BatchSize         int
	BatchIntervalInMS int64
	RunDurationInSec  int64
	TotalCount        int
	TestAPI           bool
	TestStream        bool
	APISchema         map[string]interface{}
	StreamSchema      map[string]interface{}
}

var (
	AppConfig Conf
	Schemas   map[string]Schema
)

func StartLoadTest() {
	wg := &sync.WaitGroup{}
	for identifier, schemaConf := range Schemas {
		if !schemaConf.Enabled {
			continue
		}
		wg.Add(1)
		go loadTest(identifier, schemaConf, wg)
	}
	wg.Wait()
	fmt.Printf("\n\nDone!\n\n")
}

func loadTest(schemaName string, schemaConf Schema, wg *sync.WaitGroup) {
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
		apiMsgs, streamMsgs := getNextDataBatch(nextBatchSize, schemaName, schemaConf)
		call(schemaName, apiMsgs)
		publish(schemaName, streamMsgs)
		currCount += len(streamMsgs)
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

func getNextDataBatch(batchSize int, schemaName string, schemaConf Schema) ([][]byte, []*sarama.ProducerMessage) {
	apiMsgs := make([][]byte, 0, batchSize)
	streamMsgs := make([]*sarama.ProducerMessage, 0, batchSize)
	topic := AppConfig.KafkaConfigs[schemaName].Topic

	for count := 0; count < batchSize; count++ {
		apiData := getDataFromSchema(schemaConf.APISchema)
		apiMsg, err := json.Marshal(apiData)
		if err != nil {
			fmt.Printf("\nError while unmarshaling data for %s, err: %v", schemaName, err)
			continue
		}
		streamData := getDataFromSchema(schemaConf.StreamSchema)
		streamMsg, err := json.Marshal(streamData)
		if err != nil {
			fmt.Printf("\nError while unmarshaling data for %s, err: %v", schemaName, err)
			continue
		}
		streamMsgs = append(streamMsgs, &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(streamMsg),
		})
		apiMsgs = append(apiMsgs, apiMsg)
	}
	return apiMsgs, streamMsgs
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
