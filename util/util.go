package util

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func Retry(attempts int, timeout time.Duration, fn func() error) (err error) {
	for i := 0; ; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		if i >= (attempts - 1) {
			break
		}
		time.Sleep(timeout)
	}
	return err
}

func LoadJSON(file string, to interface{}) error {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, to)
	if err != nil {
		return err
	}
	return nil
}

func WaitForSignal() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
	<-interrupt
}

func StringUTC(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z07:00")
}
