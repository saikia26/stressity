package loadtest

import (
	"bytes"
	"net/http"
	"sync"
	"time"
)

const (
	contentType     = "Content-Type"
	applicationJSON = "application/json"
)

type APIClient struct {
	Client     *http.Client
	Method     string
	URL        string
	Headers    map[string]string
	NumClients int
}

var (
	apiClients = make(map[string]APIClient)
)

func InitHTTPClients() {
	for apiName, conf := range AppConfig.APIConfigs {
		if !conf.Enabled {
			continue
		}
		apiClient := createAPIClients(conf)
		apiClients[apiName] = apiClient
	}
}

func createAPIClients(config APIConfig) APIClient {
	// Maximum Idle connections across all hosts
	maxIdleCon := config.MaxIdleConnections
	if maxIdleCon == 0 {
		maxIdleCon = 100
	}
	maxIdleConPerHost := config.MaxIdleConnectionsPerHost
	if maxIdleConPerHost == 0 {
		maxIdleConPerHost = 30
	}
	timeoutInMs := config.TimeoutInMS
	if timeoutInMs == 0 {
		timeoutInMs = 2000
	}
	return APIClient{
		Client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        maxIdleCon,
				MaxIdleConnsPerHost: maxIdleConPerHost,
			},
			Timeout: time.Duration(timeoutInMs) * time.Millisecond,
		},
		Method: config.Method,
		URL:    config.URL,
		Headers: map[string]string{
			contentType: applicationJSON,
		},
		NumClients: config.NumClients,
	}
}

func callForABatch(apiName string, msgs [][]byte) {
	apiClient, _ := apiClients[apiName]
	headers := http.Header{}
	for key, value := range apiClient.Headers {
		headers.Set(key, value)
	}
	msgQueue := make(chan []byte, 50)
	respChan := make(chan http.Response, len(msgs))
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		for _, msg := range msgs {
			msgQueue <- msg
		}
	}()

	go func() {
		for i := 0; i < len(msgs); i++ {
			<-respChan
		}
		wg.Done()
	}()

	for i := 0; i < apiClient.NumClients; i++ {
		go func() {
			for msg := range msgQueue {
				request, _ := http.NewRequest(apiClient.Method, apiClient.URL, bytes.NewBuffer(msg))
				request.Header = headers
				apiClient.Client.Do(request)
				respChan <- http.Response{}
			}
		}()
	}

	wg.Wait()
}
