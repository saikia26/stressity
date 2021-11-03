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
	base         = "base"
	prefix       = "prefix"
	suffix       = "suffix"
	min          = "min"
	max          = "max"
	arrayVal     = "val"
	customFormat = "customFormat"
	customVal    = "customVal"
	outputFormat = "outputFormat"
	outputType   = "outputType"

	randomBaseType = "random"
	uuidBaseType   = "uuid"

	separator            = ":"
	defaultRange float64 = 100000
)

var (
	valueFinders = map[string]func(meta map[string]interface{}) interface{}{
		"string":             getStringVal,
		"uuid":               getUUID,
		"number":             getNumber,
		"integer":            getInteger,
		"time":               getTime,
		"getRandomFromArray": getRandomFromArray,
	}
)

func getStringVal(meta map[string]interface{}) interface{} {
	prefix, _ := meta[prefix].(string)
	suffix, _ := meta[suffix].(string)
	base, _ := meta[base].(string)
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
	minVal, _ := meta[min].(float64)
	maxVal, _ := meta[max].(float64)
	numRange := maxVal - minVal
	if numRange <= 0 {
		numRange = defaultRange
	}
	return minVal + (numRange * rand.Float64())
}

func getInteger(meta map[string]interface{}) interface{} {
	minVal, _ := meta[min].(float64)
	maxVal, _ := meta[max].(float64)
	minInt := int64(minVal)
	maxInt := int64(maxVal)
	numRange := maxInt - minInt
	if numRange <= 0 {
		numRange = int64(defaultRange)
	}
	return minInt + rand.Int63n(numRange)
}

func getTime(meta map[string]interface{}) interface{} {
	format, okFormat := meta[customFormat].(string)
	customVal, okCustomVal := meta[customVal].(string)
	outputFormat, okOutputFormat := meta[outputFormat].(string)

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

func getRandomFromArray(meta map[string]interface{}) interface{} {
	val := meta[arrayVal].([]interface{})
	randomIndex := rand.Intn(len(val))
	return val[randomIndex]
}
