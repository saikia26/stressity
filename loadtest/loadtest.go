package loadtest

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

const (
	keyType        = "type"
	keyLen         = "len"
	keyRawVal      = "rawVal"
	keyMeta        = "meta"
	keyObject      = "object"
	keyArray       = "array"
	keyObjectMap   = "objectMap"
	keyArrayValues = "arrayValues"
)

var (
	AppConfig AppConf
	Features  map[string]FeatureConf
)

func StartLoadTest() {
	wg := &sync.WaitGroup{}
	for identifier, featureConf := range Features {
		if !featureConf.Enabled {
			continue
		}
		wg.Add(1)
		go loadTest(identifier, featureConf, wg)
	}
	wg.Wait()
	fmt.Printf("\n\nDone!\n\n")
}

func loadTest(featureName string, featureConf FeatureConf, wg *sync.WaitGroup) {
	startTime := time.Now()
	currCount := 0
	batchCount := 0
	defer func() {
		fmt.Printf("\n\nLoad test for %s completed!\nTotal count: %d\nStart time: %s\nEnd time: %s\n", featureName, currCount, startTime.String(), time.Now().String())
		wg.Done()
	}()

	endTime := startTime.Add(time.Duration(featureConf.RunDurationInSec) * time.Second)
	maxCount := featureConf.TotalCount
	batchSize := featureConf.BatchSize
	sleepDuration := time.Duration(featureConf.BatchIntervalInMS) * time.Millisecond

	for currCount < maxCount && time.Now().Before(endTime) {
		batchCount++
		fmt.Printf("\nProcessing %s batch %d | Total done till now - %d", featureName, batchCount, currCount)
		nextBatchSize := getNextBatchSize(currCount, maxCount, batchSize)
		sampleData := generateDataFromKeyMeta(featureConf.KeyMeta, nextBatchSize)
		for schemaName, schemaAttributes := range featureConf.APISchema {
			if !schemaAttributes.Enabled {
				continue
			}
			msgs := getNextDataBatchForAPI(schemaName, schemaAttributes.Definition, sampleData)
			go callForABatch(schemaName, msgs)
		}
		for schemaName, schemaAttributes := range featureConf.StreamSchema {
			if !schemaAttributes.Enabled {
				continue
			}
			msgs := getNextDataBatchForStream(schemaName, schemaAttributes.Definition, sampleData)
			go publishForABatch(schemaName, msgs)
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
		typ, ok := valMap[keyType]
		if ok {
			if typ == keyObject {
				res[key] = getDataFromSchema(valMap[keyObjectMap].(map[string]interface{}), sampleData)
				continue
			}
			if typ == keyArray {
				// hacky way to convert float to int
				arrLen := valMap[keyLen].(float64)
				arr := make([]interface{}, 0, int(arrLen))
				arrSchema := valMap[keyArrayValues].(map[string]interface{})
				for i := 0; i < int(arrLen); i++ {
					arr = append(arr, getDataFromSchema(arrSchema[keyObjectMap].(map[string]interface{}), sampleData))
				}
				res[key] = arr
				continue
			}
		}
		res[key] = sampleData[key]
	}
	return res
}
