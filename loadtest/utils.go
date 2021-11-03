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

func ValidatePreRequisites(features map[string]FeatureConf) error {
	featuresToRun := 0
	for identifier, featureConf := range features {
		if !featureConf.Enabled {
			continue
		}
		err := validateSchemaForFeature(identifier, featureConf)
		if err != nil {
			return err
		}
		featuresToRun++
	}
	if featuresToRun == 0 {
		return errors.New("no features present")
	}
	return nil
}

func validateSchemaForFeature(featureName string, featureConf FeatureConf) error {
	for schemaName, schemaAttributes := range featureConf.StreamSchema {
		if !schemaAttributes.Enabled {
			continue
		}
		_, ok := AppConfig.KafkaConfigs[schemaName]
		if !ok {
			return fmt.Errorf("kafka config is not present for schema %s in feature config for %s", schemaName, featureName)
		}
		err := validateSchema(featureName, schemaName, schemaAttributes.Definition, featureConf.KeyMeta)
		if err != nil {
			return err
		}
	}
	for schemaName, schemaAttributes := range featureConf.APISchema {
		if !schemaAttributes.Enabled {
			continue
		}
		_, ok := AppConfig.APIConfigs[schemaName]
		if !ok {
			return fmt.Errorf("api config is not present for schema %s in feature config for %s", schemaName, featureName)
		}
		err := validateSchema(featureName, schemaName, schemaAttributes.Definition, featureConf.KeyMeta)
		if err != nil {
			return err
		}
	}
	return nil
}

func validateSchema(featureName string, schemaName string, schema map[string]interface{}, metaObj map[string]interface{}) error {
	for key, valObj := range schema {
		valMap, ok := valObj.(map[string]interface{})
		if !ok {
			return fmt.Errorf("non-map value found for key %s in schema %s for feature %s", key, schemaName, featureName)
		}

		objectTyp, ok := valMap[Type]
		if ok {
			if objectTyp.(string) == Object {
				err := validateObjectMap(valMap, featureName, schemaName, metaObj, key)
				if err != nil {
					return err
				}
				continue
			}

			if objectTyp.(string) == Array {
				err := validateArray(valMap, featureName, schemaName, metaObj, key)
				if err != nil {
					return err
				}
				continue
			}
		}

		metaIdentifier := fetchMetaIdentifier(valMap, key)
		metaMap, ok := metaObj[metaIdentifier].(map[string]interface{})
		if !ok {
			return fmt.Errorf("meta not found for key %s in schema %s for feature %s", key, schemaName, featureName)
		}
		if _, ok := metaMap[RawVal]; ok {
			continue
		}
		keyTyp, ok := metaMap[Type]
		if !ok {
			return fmt.Errorf("type not found for key %s in key meta for schema %s for feature %s", key, schemaName, featureName)
		}
		if _, ok := valueFinders[keyTyp.(string)]; !ok {
			return fmt.Errorf("typ %s not supported (key %s in schema %s for feature %s)", objectTyp, key, schemaName, featureName)
		}
		if keyTyp.(string) == "uuid" || keyTyp.(string) == "time" {
			continue
		}
		meta, ok := metaMap[Meta]
		if !ok {
			return fmt.Errorf("meta not found for key %s in key meta for schema %s for feature %s", key, schemaName, featureName)
		}
		if _, ok := meta.(map[string]interface{}); !ok {
			return fmt.Errorf("meta not a map for key %s in schema %s for feature %s", key, schemaName, featureName)
		}
	}
	return nil
}

func fetchMetaIdentifier(schemaObj map[string]interface{}, schemaKey string) (key string) {
	if val, ok := schemaObj[MetaKey]; ok {
		return val.(string)
	}
	return schemaKey
}

// validateArray currently supports and validates an array of key value pairs only.
func validateArray(valMap map[string]interface{}, featureName, schemaName string, metaObj map[string]interface{}, key string) error {
	arr, ok := valMap[ArrayValues]
	if !ok {
		return fmt.Errorf("no array schema found for key %s in schema %s for feature %s", key, schemaName, featureName)
	}
	_, ok = valMap[Len]
	if !ok {
		return fmt.Errorf("array length not specified for key %s in schema %s for feature %s", key, schemaName, featureName)
	}
	arrVals, ok := arr.(map[string]interface{})
	if !ok {
		return fmt.Errorf("non-map object found for key %s in schema %s for feature %s", key, schemaName, featureName)
	}

	return validateObjectMap(arrVals, featureName, schemaName, metaObj, key)
}

func validateObjectMap(valMap map[string]interface{}, featureName, schemaName string, metaObj map[string]interface{}, key string) error {
	objectMap, ok := valMap[ObjectMap]
	if !ok {
		return fmt.Errorf("no object map found for key %s in schema %s for feature %s", key, schemaName, featureName)
	}

	objectMapVal, ok := objectMap.(map[string]interface{})
	if !ok {
		return fmt.Errorf("non-map object found for object type key %s in schema %s for feature %s", key, schemaName, featureName)
	}

	return validateSchema(featureName, schemaName, objectMapVal, metaObj)
}

func getRandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}
