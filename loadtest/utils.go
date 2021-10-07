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
		err := validateSchema(schemaName, schemaConf.StreamSchema, schemaConf.KeyMeta)
		if err != nil {
			return err
		}
	}

	_, ok = AppConfig.KafkaConfigs[schemaName]
	if schemaConf.TestAPI {
		if !ok {
			return fmt.Errorf("kafka config is either not present for %s", schemaName)
		}
		err := validateSchema(schemaName, schemaConf.APISchema, schemaConf.KeyMeta)
		if err != nil {
			return err
		}
	}

	return nil
}

func validateSchema(schemaName string, schema map[string]interface{}, metaObj map[string]interface{}) error {
	for key, valObj := range schema {
		valMap, ok := valObj.(map[string]interface{})
		if !ok {
			return fmt.Errorf("non-map value found for key %s in schema %s", key, schemaName)
		}

		objectTyp, ok := valMap[keyType]
		if ok && objectTyp.(string) == keyObject {
			objectMap, ok := valMap[keyObjectMap]
			if !ok {
				return fmt.Errorf("no object map found for object type key %s in schema %s", key, schemaName)
			}

			objectMapVal, ok := objectMap.(map[string]interface{})
			if !ok {
				return fmt.Errorf("non-map object found for object type key %s in schema %s", key, schemaName)
			}

			err := validateSchema(schemaName, objectMapVal, metaObj)
			if err != nil {
				return err
			}
			continue
		}

		metaMap, ok := metaObj[key].(map[string]interface{})
		if !ok {
			return fmt.Errorf("meta not found for key %s in schema %s", key, schemaName)
		}
		if _, ok := metaMap[keyRawVal]; ok {
			continue
		}
		keyTyp, ok := metaMap[keyType]
		if !ok {
			return fmt.Errorf("type not found for key %s in key meta for schema %s", key, schemaName)
		}
		if _, ok := valueFinders[keyTyp.(string)]; !ok {
			return fmt.Errorf("typ %s not supported (key %s in schema %s)", objectTyp, key, schemaName)
		}
		if keyTyp.(string) == "uuid" || keyTyp.(string) == "time" {
			continue
		}
		meta, ok := metaMap[keyMeta]
		if !ok {
			return fmt.Errorf("meta not found for key %s in key meta for schema %s", key, schemaName)
		}
		if _, ok := meta.(map[string]interface{}); !ok {
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
