package handler

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/denistoikka/filebeat/index"
	"github.com/denistoikka/filebeat/metadata"
	"github.com/denistoikka/filebeat/parser"
	"github.com/denistoikka/filebeat/util"
)

type LogHandlerConfig struct {
	FilePattern string `json:"filePattern"`
	Directory   string `json:"directory"`
}

type Handler struct {
	metadata    *metadata.LogMetadataStorage
	index       *index.LogIndex
	parser      *parser.Parser
	filePattern *regexp.Regexp
	directory   string
	isActive    bool
}

func NewHandler(conf LogHandlerConfig, metadataStorage *metadata.LogMetadataStorage, logIndex *index.LogIndex) *Handler {
	if conf.FilePattern == "" {
		conf.FilePattern = `\.log\.INFO\.`
	}
	if conf.Directory == "" {
		conf.Directory = "/logs"
	}
	h := &Handler{
		metadata:    metadataStorage,
		index:       logIndex,
		filePattern: regexp.MustCompile(conf.FilePattern),
		directory:   conf.Directory,
		isActive:    true,
	}
	h.parser = parser.NewParser(h.handleLogEntry)
	return h
}

func (h *Handler) Stop() {
	h.isActive = false
	h.parser.Stop()
}

func (h *Handler) Start() {
	fmt.Printf("Watching directory (%s) for log file pattern (%s)\n", h.directory, h.filePattern.String())
	firstRun := true
	for h.isActive {
		if !firstRun {
			// all files handled - wait a little bit until the next run
			time.Sleep(time.Second * 10)
		} else {
			firstRun = false
		}

		if err := h.handle(); err != nil {
			fmt.Println(err)
		}
	}
}

func (h *Handler) handle() error {
	return filepath.Walk(h.directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("filepath.Walk error: %v", err)
		}
		if info.IsDir() {
			return nil
		}
		if match := h.filePattern.FindString(path); match == "" {
			return nil
		}
		return h.handleFile(path, info)
	})
}

type FileMetadata struct {
	Path         string    `json:"path"`
	LastModified time.Time `json:"lastModified"`
}

func (h *Handler) handleFile(path string, info os.FileInfo) error {
	var discard int
	if fm, err := h.metadata.GetFileMetadata(path); err == nil {
		if !fm.LastModified.Before(info.ModTime()) {
			// no changes in file, skip
			return nil
		}
		discard = fm.Discard
	}

	fmt.Println("Handling:", path)

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	bytesRead, err := h.parser.ParseLogFile(file, discard)
	if err != nil {
		return err
	}

	return h.metadata.SetFileMetadata(metadata.FileMetadata{
		Path:         path,
		LastModified: info.ModTime(),
		Discard:      discard + bytesRead,
	})
}

func (h *Handler) handleLogEntry(entry *parser.ParseLogEntry) error {
	if len(entry.MessageLines) > 0 {
		return h.addEntryToLog(entry)
	}
	return nil
}

func (h *Handler) addEntryToLog(entry *parser.ParseLogEntry) error {
	message := strings.Join(entry.MessageLines, "\n")
	key := util.StringUTC(entry.Timestamp) + "/" + hash(message)
	return h.index.AddEntry(index.IndexLogEntry{
		Key:           key,
		Severity:      entry.Severity,
		Timestamp:     entry.Timestamp,
		EntryPoint:    entry.EntryPoint,
		FileName:      entry.LogFile.FileName,
		ContainerName: entry.LogFile.Container,
		Message:       message,
	})
}

func hash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	return strconv.FormatUint(uint64(h.Sum32()), 10)
}
