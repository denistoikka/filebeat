package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/denistoikka/filebeat/handler"
	"github.com/denistoikka/filebeat/index"
	"github.com/denistoikka/filebeat/metadata"
	"github.com/denistoikka/filebeat/util"
)

type Config struct {
	IndexConfig   index.LogIndexConfig     `json:"indexConfig"`
	HandlerConfig handler.LogHandlerConfig `json:"handlerConfig"`
}

func main() {
	configFileName := flag.String("config", "/config/logging.json", "configuration file")
	flag.Parse()

	config := getConfigFromFile(*configFileName)
	logIndex := getLogIndex(config)
	metadataStorage := getMetadataStorage()
	defer metadataStorage.Close()

	h := handler.NewHandler(config.HandlerConfig, metadataStorage, logIndex)
	go h.Start()

	util.WaitForSignal()
	h.Stop()
}

func getConfigFromFile(fileName string) Config {
	var config Config
	if err := util.LoadJSON(fileName, &config); err != nil {
		panic(fmt.Errorf("%v: Cannot load config json: %v", time.Now(), err))
	}
	return config
}

// Returns elastic search index in order to save log entries.
func getLogIndex(config Config) *index.LogIndex {
	logIndex, err := index.NewLogIndex(config.IndexConfig)
	if err != nil {
		panic(fmt.Errorf("%v: Cannot open log index: %v", time.Now(), err))
	}
	return logIndex
}

// Returns storage to keep metadata information of files that were handled by filebeat.
func getMetadataStorage() *metadata.LogMetadataStorage {
	metadataStorage, err := metadata.NewMetadataStorage()
	if err != nil {
		panic(fmt.Errorf("%v: Cannot open metadata storage: %v", time.Now(), err))
	}
	return metadataStorage
}
