package index

import (
	"fmt"
	"time"

	"github.com/denistoikka/filebeat/util"

	"golang.org/x/net/context"
	"gopkg.in/olivere/elastic.v6"
)

const LogIndexType = "log"

const LogIndexSettings = `{
	"settings": {
		"analysis": {
			"analyzer": {
				"analyzer_keyword":{
					"tokenizer": "keyword",
					"filter": "lowercase"
				}
			}
		}
	}
}`

const LogIndexMapping = `{
	"_all": {
		"enabled": false
	},
	"properties": {
		"key": {
			"type": "keyword"
		},
		"severity": {
			"type": "text",
			"analyzer": "analyzer_keyword"
		},
		"timestamp": {
			"type": "date"
		},
		"entryPoint": {
			"type": "text"
		},
		"fileName": {
			"type": "text"
		},
		"containerName": {
			"type": "text"
		},
		"message": {
			"type": "text"
		}
	}
}`

type LogIndexConfig struct {
	Index string   `json:"index"`
	Sniff bool     `json:"sniff"`
	Urls  []string `json:"urls"`
}

type LogIndex struct {
	index string
	esc   *elastic.Client
}

func NewLogIndex(conf LogIndexConfig) (*LogIndex, error) {
	if conf.Index == "" {
		conf.Index = "logging"
	}
	if len(conf.Urls) == 0 {
		conf.Urls = []string{"http://elasticsearch:9200"}
	}

	esc, err := newElasticIndex(conf)
	if err != nil {
		return nil, err
	}

	return &LogIndex{index: conf.Index, esc: esc}, nil
}

func newElasticIndex(conf LogIndexConfig) (esc *elastic.Client, err error) {
	esc, err = newElasticClient(elastic.SetSniff(conf.Sniff), elastic.SetURL(conf.Urls...))
	if err != nil {
		return nil, err
	}
	if err = createIndexIfNotExists(esc, conf.Index); err != nil {
		return nil, err
	}
	if err = setupIndex(esc, conf.Index); err != nil {
		return nil, err
	}
	return esc, nil
}

func newElasticClient(options ...elastic.ClientOptionFunc) (esc *elastic.Client, err error) {
	options = append(options, elastic.SetErrorLog(&logger{}))
	err = util.Retry(10, time.Second*10, func() error {
		var _err error
		esc, _err = elastic.NewClient(options...)
		return _err
	})
	return
}

func createIndexIfNotExists(esc *elastic.Client, index string) error {
	exists, err := esc.IndexExists(index).Do(context.Background())
	if err != nil {
		return err
	}
	if !exists {
		if _, err := esc.CreateIndex(index).Do(context.Background()); err != nil {
			return fmt.Errorf("Failed to create LogIndex '%s': %v", index, err)
		}
	}
	return nil
}

func setupIndex(esc *elastic.Client, index string) error {
	return funcWithSuspendedIndex(esc, index, func() error {
		putSettings := elastic.NewIndicesPutSettingsService(esc).Index(index).BodyString(LogIndexSettings)
		if _, err := putSettings.Do(context.Background()); err != nil {
			return err
		}
		putMapping := elastic.NewPutMappingService(esc).Index(index).Type(LogIndexType).BodyString(LogIndexMapping)
		if _, err := putMapping.Do(context.Background()); err != nil {
			return err
		}
		return nil
	})
}

// funcWithSuspendedIndex temporary closes LogIndex in order to make some operations with it.
func funcWithSuspendedIndex(esc *elastic.Client, index string, fn func() error) error {
	err := util.Retry(10, time.Second, func() error {
		return closeIndex(esc, index)
	})
	if err != nil {
		return fmt.Errorf("Failed to close LogIndex: %v", err)
	}
	if err := fn(); err != nil {
		return err
	}
	if err := openIndex(esc, index); err != nil {
		return fmt.Errorf("Failed to open LogIndex: %v", err)
	}
	return nil
}

func openIndex(esc *elastic.Client, index string) error {
	_, err := esc.OpenIndex(index).Do(context.Background())
	return err
}

func closeIndex(esc *elastic.Client, index string) error {
	_, err := esc.CloseIndex(index).Do(context.Background())
	return err
}

func (li *LogIndex) AddEntry(entry IndexLogEntry) error {
	_, err := li.esc.Index().Index(li.index).Type(LogIndexType).Id(entry.Key).BodyJson(&entry).Do(context.Background())
	return err
}

type IndexLogEntry struct {
	Key           string    `json:"key"`
	Severity      string    `json:"severity"`
	Timestamp     time.Time `json:"timestamp"`
	EntryPoint    string    `json:"entryPoint"`
	FileName      string    `json:"fileName"`
	ContainerName string    `json:"containerName"`
	Message       string    `json:"message"`
}

type logger struct{}

func (l *logger) Printf(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
}
