package loadtest

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
)

func DecodeFile(filePath string, destination interface{}) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(file)
	return decoder.Decode(destination)
}

func ValidatePreRequisites(schemas map[string]Schema) error {
	schemasToRun := 0
	for identifier, schemaConf := range schemas {
		if !schemaConf.Enabled {
			continue
		}
		err := validateConfigsForSchema(identifier, schemaConf)
		if err != nil {
			return err
		}
		schemasToRun++
	}
	if schemasToRun == 0 {
		return errors.New("no schemas present")
	}
	return nil
}

func validateConfigsForSchema(schemaName string, schemaConf Schema) error {
	_, ok := AppConfig.KafkaConfigs[schemaName]
	if schemaConf.TestStream {
		if !ok {
			return fmt.Errorf("kafka config is either not present for %s", schemaName)
		}
		err := validateSchema(schemaName, schemaConf.StreamSchema)
		if err != nil {
			return err
		}
	}

	_, ok = AppConfig.KafkaConfigs[schemaName]
	if schemaConf.TestAPI {
		if !ok {
			return fmt.Errorf("kafka config is either not present for %s", schemaName)
		}
		err := validateSchema(schemaName, schemaConf.APISchema)
		if err != nil {
			return err
		}
	}

	return nil
}

func validateSchema(schemaName string, schema map[string]interface{}) error {
	for key, valObj := range schema {
		valMap, ok := valObj.(map[string]interface{})
		if !ok {
			return fmt.Errorf("non-map value found for key %s in schema %s", key, schemaName)
		}
		if _, ok := valMap[keyRawVal]; ok {
			continue
		}
		typ, ok := valMap[keyType].(string)
		if !ok {
			return fmt.Errorf("type not present for key %s in schema %s", key, schemaName)
		}
		if typ == keyObject {
			objectMap, ok := valMap[keyObjectMap].(map[string]interface{})
			if !ok {
				return fmt.Errorf("non-map object found for object type key %s in schema %s", key, schemaName)
			}
			err := validateSchema(schemaName, objectMap)
			if err != nil {
				return err
			}
			continue
		}
		if _, ok := valueFinders[typ]; !ok {
			return fmt.Errorf("typ %s not supported (key %s in schema %s)", typ, key, schemaName)
		}
		if typ == "uuid" || typ == "time" {
			continue
		}
		if _, ok := valMap[keyMeta].(map[string]interface{}); !ok {
			return fmt.Errorf("meta not a map for key %s in schema %s", key, schemaName)
		}
	}
	return nil
}

func getRandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}
