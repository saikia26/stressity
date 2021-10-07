package loadtest

import (
	"bytes"
	"net/http"
	"time"
)

const (
	contentType     = "Content-Type"
	applicationJSON = "application/json"
)

type APIClient struct {
	Client  *http.Client
	Method  string
	URL     string
	Headers map[string]string
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
	timeoutInMs := config.RequestTimeOut
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
	}
}

func callForABatch(apiName string, msgs [][]byte) (errs []error) {
	apiClient, _ := apiClients[apiName]
	headers := http.Header{}
	for key, value := range apiClient.Headers {
		headers.Set(key, value)
	}

	for _, msg := range msgs {
		request, err := http.NewRequest(apiClient.Method, apiClient.URL, bytes.NewBuffer(msg))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		request.Header = headers
		_, err = apiClient.Client.Do(request)
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}
	return errs
}
