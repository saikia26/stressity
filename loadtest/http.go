package loadtest

import (
	"net/url"
	"sync"
	"time"

	"github.com/valyala/fasthttp"
)

const (
	contentType     = "Content-Type"
	applicationJSON = "application/json"
)

type apiClient struct {
	Clients        []fasthttp.PipelineClient
	Method         string
	Timeout        time.Duration
	Headers        map[string]string
	PipelineFactor int
	URL            string
}

var (
	apiClients = make(map[string]apiClient)
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

func createAPIClients(config APIConfig) apiClient {
	var clients []fasthttp.PipelineClient
	u, _ := url.Parse(config.URL)
	client := fasthttp.PipelineClient{
		Addr:               u.Host,
		IsTLS:              u.Scheme == "https",
		MaxPendingRequests: config.PipelineFactor,
	}

	for i := 0; i < config.NumClients; i++ {
		clients = append(clients, client)
	}
	headers := map[string]string{
		contentType: applicationJSON,
	}
	return apiClient{
		Clients:        clients,
		Method:         config.Method,
		Timeout:        time.Duration(config.TimeoutInMS) * time.Millisecond,
		Headers:        headers,
		PipelineFactor: config.PipelineFactor,
		URL:            config.URL,
	}
}

func callForABatch(apiName string, msgs [][]byte) {
	apiClient, _ := apiClients[apiName]
	msgQueue := make(chan []byte, 50)
	respChan := make(chan bool, len(msgs))
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

	for _, client := range apiClient.Clients {
		for j := 0; j < apiClient.PipelineFactor; j++ {
			go func() {
				for msg := range msgQueue {
					req := fasthttp.AcquireRequest()
					req.SetBody(msg)
					req.Header.SetMethodBytes([]byte(apiClient.Method))
					for key, value := range apiClient.Headers {
						req.Header.Add(key, value)
					}
					req.SetRequestURI(apiClient.URL)
					res := fasthttp.AcquireResponse()
					client.DoTimeout(req, res, apiClient.Timeout)
					respChan <- true
				}
			}()
		}
	}

	wg.Wait()
	return
}
