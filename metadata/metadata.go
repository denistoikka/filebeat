package metadata

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

const metadataBucket = "metadata"

type LogMetadataStorage struct {
	bolt *bolt.DB
}

func NewMetadataStorage() (*LogMetadataStorage, error) {
	boltDb, err := bolt.Open("/metadata/metadata.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	err = boltDb.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(metadataBucket)); err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		boltDb.Close()
		return nil, err
	}
	return &LogMetadataStorage{bolt: boltDb}, nil
}

func (m *LogMetadataStorage) Close() {
	m.bolt.Close()
}

type FileMetadata struct {
	Path         string    `json:"path"`
	LastModified time.Time `json:"lastModified"`
	Discard      int       `json:"discard"`
}

func (m *LogMetadataStorage) GetFileMetadata(path string) (*FileMetadata, error) {
	var fileMetadata FileMetadata
	err := m.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metadataBucket))
		v := b.Get([]byte(path))
		return json.Unmarshal(v, &fileMetadata)
	})
	if err != nil {
		return nil, err
	}
	return &fileMetadata, nil
}

func (m *LogMetadataStorage) SetFileMetadata(fileMetadata FileMetadata) error {
	return m.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metadataBucket))
		value, err := json.Marshal(&fileMetadata)
		if err != nil {
			return err
		}
		return b.Put([]byte(fileMetadata.Path), value)
	})
}
