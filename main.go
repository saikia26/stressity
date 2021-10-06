package main

import "github.com/stressity/loadtest"

const (
	configFilePath = "config.json"
	schemaFilePath = "stream_schemas.json"
)

func main() {
	err := loadtest.DecodeFile(configFilePath, &loadtest.AppConfig)
	if err != nil {
		panic(err)
	}
	err = loadtest.DecodeFile(schemaFilePath, &loadtest.Schemas)
	if err != nil {
		panic(err)
	}

	err = loadtest.InitProducers()
	if err != nil {
		panic(err)
	}
	loadtest.InitHTTPClients()
	err = loadtest.ValidatePreRequisites(loadtest.Schemas)
	if err != nil {
		panic(err)
	}
	loadtest.StartLoadTest()
}
