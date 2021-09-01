package loadtest

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	baseKey         = "base"
	prefixKey       = "prefix"
	suffixKey       = "suffix"
	minKey          = "min"
	maxKey          = "max"
	customFormatKey = "customFormat"
	customValKey    = "customVal"
	outputFormatKey = "outputFormat"
	outputType      = "outputType"

	randomBaseType = "random"
	uuidBaseType   = "uuid"

	separator            = ":"
	defaultRange float64 = 100000
)

var (
	valueFinders = map[string]func(meta map[string]interface{}) interface{}{
		"string":  getStringVal,
		"uuid":    getUUID,
		"number":  getNumber,
		"integer": getInteger,
		"time":    getTime,
	}
)

func getStringVal(meta map[string]interface{}) interface{} {
	prefix, _ := meta[prefixKey].(string)
	suffix, _ := meta[suffixKey].(string)
	base, _ := meta[baseKey].(string)
	params := strings.Split(base, separator)

	var baseVal string
	switch params[0] {
	case randomBaseType:
		n := 10
		if len(params) > 1 {
			k, err := strconv.Atoi(params[1])
			if err == nil {
				n = k
			}
		}
		baseVal = getRandomString(n)
	case uuidBaseType:
		baseVal = uuid.NewString()
	default:
		baseVal = base
	}
	return fmt.Sprintf("%s%s%s", prefix, baseVal, suffix)
}

func getUUID(meta map[string]interface{}) interface{} {
	return uuid.NewString()
}

func getNumber(meta map[string]interface{}) interface{} {
	min, _ := meta[minKey].(float64)
	max, _ := meta[maxKey].(float64)
	numRange := max - min
	if numRange <= 0 {
		numRange = defaultRange
	}
	return min + (numRange * rand.Float64())
}

func getInteger(meta map[string]interface{}) interface{} {
	min, _ := meta[minKey].(float64)
	max, _ := meta[maxKey].(float64)
	minInt := int64(min)
	maxInt := int64(max)
	numRange := maxInt - minInt
	if numRange <= 0 {
		numRange = int64(defaultRange)
	}
	return minInt + rand.Int63n(numRange)
}

func getTime(meta map[string]interface{}) interface{} {
	format, okFormat := meta[customFormatKey].(string)
	customVal, okCustomVal := meta[customValKey].(string)
	outputFormat, okOutputFormat := meta[outputFormatKey].(string)

	resTime := time.Now()
	if okFormat && okCustomVal {
		parsedTime, err := time.Parse(format, customVal)
		if err == nil {
			resTime = parsedTime
		}
	}

	if okOutputFormat {
		var output interface{}
		switch outputFormat {
		case "epoch":
			fallthrough
		case "epochSec":
			output = resTime.Unix()
		case "epochMS":
			output = resTime.Unix() * 1000
		case "epochNS":
			output = resTime.UnixNano()
		default:
			output = resTime.Format(outputFormat)
		}
		if outputTyp, _ := meta[outputType]; outputTyp == "string" {
			output = fmt.Sprintf("%v", output)
		}
		return output
	}
	return resTime
}
