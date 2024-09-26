package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *badger.DB
	conf   *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		conf:   conf,
		engine: engine_util.CreateDB(path.Join(conf.DBPath, "kv"), false),
	}
}

func (s *StandAloneStorage) Start() error {
	if s.engine == nil {
		return storage.ErrEngineNotInitiate
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if s.engine != nil {
		return s.engine.Close()
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.engine.NewTransaction(false)
	return NewStandAloneStorageReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// init a badger transaction to update batch
	return s.engine.Update(func(txn *badger.Txn) error {
		for _, mod := range batch {
			switch mod.Data.(type) {
			case storage.Put:
				if err := txn.Set(engine_util.KeyWithCF(mod.Cf(), mod.Key()), mod.Value()); err != nil {
					return err
				}
			case storage.Delete:
				if err := txn.Delete(engine_util.KeyWithCF(mod.Cf(), mod.Key())); err != nil && err != badger.ErrKeyNotFound {
					return err
				}
			}
		}
		return nil
	})
}

// StandAloneStorageReader a reader implements StorageReader for standalone storage
type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		txn: txn,
	}
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(sr.txn, cf, key)
	// filter out key not found error
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *StandAloneStorageReader) Close() {
	sr.txn.Discard()
}
