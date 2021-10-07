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

	APISchemaType    = "apiSchemaType"
	StreamSchemaType = "streamSchemaType"
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
	KeyMeta           map[string]interface{}
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
		sampleData := generateDataFromKeyMeta(schemaConf.KeyMeta, nextBatchSize)
		if schemaConf.TestAPI {
			msgs := getNextDataBatchForAPI(schemaName, schemaConf.APISchema, sampleData)
			callForABatch(schemaName, msgs)
		}
		if schemaConf.TestStream {
			msgs := getNextDataBatchForStream(schemaName, schemaConf.APISchema, sampleData)
			publishForABatch(schemaName, msgs)
		}
		currCount += nextBatchSize
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

func getNextDataBatchForStream(schemaName string, schema map[string]interface{}, sampleData []map[string]interface{}) []*sarama.ProducerMessage {
	streamMsgs := make([]*sarama.ProducerMessage, 0, len(sampleData))
	topic := AppConfig.KafkaConfigs[schemaName].Topic

	for count := 0; count < len(sampleData); count++ {
		streamData := getDataFromSchema(schema, sampleData[count])
		streamMsg, err := json.Marshal(streamData)
		if err != nil {
			fmt.Printf("\nError while unmarshaling data for %s, err: %v", schemaName, err)
			continue
		}
		streamMsgs = append(streamMsgs, &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(streamMsg),
		})
	}
	return streamMsgs
}

func getNextDataBatchForAPI(schemaName string, schema map[string]interface{}, sampleData []map[string]interface{}) [][]byte {
	apiMsgs := make([][]byte, 0, len(sampleData))
	for count := 0; count < len(sampleData); count++ {
		apiData := getDataFromSchema(schema, sampleData[count])
		apiMsg, err := json.Marshal(apiData)
		if err != nil {
			fmt.Printf("\nError while unmarshaling data for %s, err: %v", schemaName, err)
			continue
		}
		apiMsgs = append(apiMsgs, apiMsg)
	}
	return apiMsgs
}

func generateDataFromKeyMeta(keysInfo map[string]interface{}, batchSize int) []map[string]interface{} {
	res := make([]map[string]interface{}, 0, batchSize)
	for count := 0; count < batchSize; count++ {
		sampleData := make(map[string]interface{})
		for key, valObj := range keysInfo {
			valMap, _ := valObj.(map[string]interface{})
			if rawVal, ok := valMap[keyRawVal]; ok {
				sampleData[key] = rawVal
				continue
			}
			sampleData[key] = valueFinders[valMap[keyType].(string)](valMap[keyMeta].(map[string]interface{}))
		}
		res = append(res, sampleData)
	}
	return res
}

func getDataFromSchema(schema map[string]interface{}, sampleData map[string]interface{}) map[string]interface{} {
	res := make(map[string]interface{})
	for key, valObj := range schema {
		valMap, _ := valObj.(map[string]interface{})
		if typ, ok := valMap[keyType]; ok && typ == keyObject {
			res[key] = getDataFromSchema(valMap[keyObjectMap].(map[string]interface{}), sampleData)
			continue
		}
		res[key] = sampleData[key]
	}
	return res
}
