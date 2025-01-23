package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn      *MvccTxn
	startKey []byte
	iter     engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite) // init write iter
	iter.Seek(startKey)
	return &Scanner{
		txn:      txn,
		startKey: startKey,
		iter:     iter,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() {
		return nil, nil, nil
	}

	// Mvvc search: greater than key + ts, if the startTs is greater than write's ts, will skip it,
	// for example, []byte{1,2,3} + 100 in db, search []byte{1,2,3} + 101, will skip this record
	scan.iter.Seek(EncodeKey(scan.startKey, scan.txn.StartTS))
	if !scan.iter.Valid() {
		return nil, nil, nil
	}

	// align start key
	decodedKey := DecodeUserKey(scan.iter.Item().KeyCopy(nil))
	if !bytes.Equal(decodedKey, scan.startKey) {
		// start key < current item's key, update start key and search again
		scan.startKey = decodedKey
		return scan.Next()
	}

	// we're going to get startKey for next search
	key := scan.startKey
	firstItem := scan.iter.Item() // Get last write based on key ascending, ts descending
	for {
		scan.iter.Next()
		if !scan.iter.Valid() {
			break
		}

		decodedKey = DecodeUserKey(scan.iter.Item().KeyCopy(nil))
		if !bytes.Equal(decodedKey, key) {
			break
		}
	}
	scan.startKey = decodedKey

	// parse write from last write
	value, err := firstItem.ValueCopy(nil)
	if err != nil {
		return nil, nil, err
	}
	write, err := ParseWrite(value)
	if err != nil {
		return nil, nil, err
	}
	if write.Kind == WriteKindDelete {
		// the delete case, we return empty value
		return key, nil, nil
	}
	// get default value from write ts
	writeValue, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	return key, writeValue, err
}
