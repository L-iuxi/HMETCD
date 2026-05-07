package db

import (
	types "TicketX/internal/type"
	"encoding/json"

	"github.com/dgraph-io/badger/v4"
)

type Store struct {
	DB *badger.DB
}

func NewStore(path string) (*Store, error) {
	opts := badger.DefaultOptions(path)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &Store{
		DB: db,
	}, nil
}

func (s *Store) Put(key string, value types.Value) error {
	return s.DB.Update(func(txn *badger.Txn) error {

		data, err := json.Marshal(value)
		if err != nil {
			return err
		}

		return txn.Set([]byte(key), data)
	})
}

func (s *Store) Get(key string) (types.Value, error) {

	var val types.Value

	err := s.DB.View(func(txn *badger.Txn) error {

		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return json.Unmarshal(data, &val)
	})

	return val, err
}
func (s *Store) Delete(key string) error {
	return s.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}
func (s *Store) Close() error {
	return s.DB.Close()
}
