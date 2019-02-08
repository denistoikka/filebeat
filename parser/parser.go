package parser

import (
	"bufio"
	"io"
	"os"
	"regexp"
	"strconv"
	"time"
)

var (
	// handle file paths like:
	// /logs/webui/fffa108f2364.log.WARNING.20190124-164334.1
	filePathRe = regexp.MustCompile(`.*/([\w]+)/[\w.]+\.log\.[\w]+\.([0-9]{8}-[0-9]{6}).*`)

	// handle entry line like:
	// E0124 16:53:43.847231 1 server.go:147] Log message
	entryLineRe = regexp.MustCompile(`^([IWEF])([0-9]{4}\s[0-9:.]+)\s+([\w])\s([\w.]+:[0-9]+)\]\s(.*)?`)
)

const (
	fileTimestampLayout  = "20060102-150405"
	entryTimestampLayout = "20060102 15:04:05.000000"
)

type Parser struct {
	handleLogEntry func(entry *ParseLogEntry) error
	isActive       bool
}

func NewParser(handleLogEntry func(entry *ParseLogEntry) error) *Parser {
	return &Parser{
		handleLogEntry: handleLogEntry,
		isActive:       true,
	}
}

func (p *Parser) Stop() {
	p.isActive = false
}

// Parses entries of provided log file.
// It discards first bits and continues from the middle of the file, if offset provided.
func (p *Parser) ParseLogFile(file *os.File, discardOffset int) (int, error) {
	currentFile := NewLogFile(file)
	reader := bufio.NewReader(file)
	reader.Discard(discardOffset)
	return p.parseLines(currentFile, reader)
}

func (p *Parser) parseLines(currentFile *LogFile, reader *bufio.Reader) (int, error) {
	currentEntry := &ParseLogEntry{
		Severity:  Severity_Info,
		Timestamp: currentFile.FileDate,
		LogFile:   currentFile,
	}
	var bytesRead int
	var forceContinueEntry bool

	for p.isActive {
		line, isPrefix, err := reader.ReadLine()
		switch err {
		case nil:
			resultEntry, isNewEntry := p.parseLine(currentEntry, string(line), forceContinueEntry)
			if isNewEntry {
				// new log entry started - handle previous entry and continue parsing
				if err = p.handleLogEntry(currentEntry); err != nil {
					return bytesRead, err
				}
				bytesRead += currentEntry.bytesRead
				currentEntry = resultEntry
			}
			forceContinueEntry = isPrefix

		case io.EOF:
			// end of file - handle previous entry and finish file parsing
			if err = p.handleLogEntry(currentEntry); err != nil {
				return bytesRead, err
			}
			bytesRead += currentEntry.bytesRead
			break

		default:
			return bytesRead, err
		}
	}

	return bytesRead, nil
}

func (p *Parser) parseLine(currentEntry *ParseLogEntry, line string, forceContinueEntry bool) (*ParseLogEntry, bool) {
	isNewEntry := false

	if forceContinueEntry {
		// log entry continues on the same line
		currentEntry.MessageLines[len(currentEntry.MessageLines)-1] += string(line)
		currentEntry.bytesRead += len(line)

	} else if matches := entryLineRe.FindStringSubmatch(string(line)); len(matches) != 6 {
		// log entry continues on new line
		currentEntry.MessageLines = append(currentEntry.MessageLines, string(line))
		currentEntry.bytesRead += len(line) + 1 // +1 for new line

	} else {
		isNewEntry = true
		prevEntry := currentEntry

		currentEntry = &ParseLogEntry{
			Severity:     logEntrySeverityLevel(matches[1]),
			Timestamp:    logEntryTimestamp(prevEntry.Timestamp, matches[2]),
			ThreadId:     matches[3],
			EntryPoint:   matches[4],
			MessageLines: []string{string(matches[5])},
			LogFile:      prevEntry.LogFile,
			bytesRead:    len(line) + 1, // +1 for new line
		}
	}
	return currentEntry, isNewEntry
}

type LogFile struct {
	FileName  string
	FilePath  string
	FileDate  time.Time
	Container string
	Entries   []*ParseLogEntry
}

func NewLogFile(file *os.File) *LogFile {
	logFile := LogFile{
		FileName: file.Name(),
		FilePath: file.Name(),
		FileDate: time.Now(),
		Entries:  []*ParseLogEntry{},
	}
	if matches := filePathRe.FindStringSubmatch(file.Name()); len(matches) == 3 {
		logFile.Container = matches[1]
		if c, err := time.Parse(fileTimestampLayout, matches[2]); err == nil {
			logFile.FileDate = c
		}
	}
	return &logFile
}

type ParseLogEntry struct {
	Severity     string
	Timestamp    time.Time
	ThreadId     string
	EntryPoint   string
	MessageLines []string
	LogFile      *LogFile
	bytesRead    int
}

const (
	Severity_Info    = "INFO"
	Severity_Warning = "WARNING"
	Severity_Error   = "ERROR"
	Severity_Fatal   = "FATAL"
)

func logEntrySeverityLevel(severityString string) string {
	switch severityString {
	case "I":
		return Severity_Info
	case "W":
		return Severity_Warning
	case "E":
		return Severity_Error
	case "F":
		return Severity_Fatal
	default:
		return Severity_Info
	}
}

func logEntryTimestamp(prevEntryTimestamp time.Time, timeString string) time.Time {
	prevEntryYear := strconv.Itoa(prevEntryTimestamp.Year())
	timestamp, err := time.Parse(entryTimestampLayout, prevEntryYear+timeString)
	if err == nil && prevEntryTimestamp.Month() > timestamp.Month() {
		// new year handling
		currentEntryYear := strconv.Itoa(prevEntryTimestamp.Year() + 1)
		timestamp, err = time.Parse(entryTimestampLayout, currentEntryYear+timeString)
	}
	if err != nil {
		timestamp = prevEntryTimestamp
	}
	return timestamp
}
