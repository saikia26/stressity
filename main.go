package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/stressity/loadtest"
)

const (
	configFilePath = "config.json"
	schemaFilePath = "schemas.json"
)

func prepareForLoadTest() {
	err := loadtest.DecodeFile(configFilePath, &loadtest.AppConfig)
	if err != nil {
		panic(err)
	}
	err = loadtest.DecodeFile(schemaFilePath, &loadtest.Features)
	if err != nil {
		panic(err)
	}
	err = loadtest.InitProducers()
	if err != nil {
		panic(err)
	}
	loadtest.InitHTTPClients()
	err = loadtest.ValidatePreRequisites(loadtest.Features)
	if err != nil {
		panic(err)
	}
}

func main() {
	prepareForLoadTest()
	router := mux.NewRouter()
	router.PathPrefix("/debug/").Handler(http.DefaultServeMux)

	srv := &http.Server{
		Addr:    ":8081",
		Handler: router,
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()
	log.Print("Server Started")

	go func() {
		loadtest.StartLoadTest()
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	}()

	<-done
	log.Print("Server Stopped")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		// extra handling here
		cancel()
	}()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server Shutdown Failed:%+v", err)
	}
	log.Print("Server Exited Properly")
}
