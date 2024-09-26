package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	defer reader.Close()
	if err != nil {
		return nil, err
	}

	if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}

	return &kvrpcpb.RawGetResponse{Value: val}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be modified
	data := storage.Modify{
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.GetCf(),
		},
	}
	err := server.storage.Write(nil, []storage.Modify{data})
	return nil, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be deleted
	data := storage.Modify{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf:  req.GetCf(),
		},
	}
	err := server.storage.Write(nil, []storage.Modify{data})
	return nil, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	defer reader.Close()
	if err != nil {
		return nil, err
	}

	resp := &kvrpcpb.RawScanResponse{
		Kvs: make([]*kvrpcpb.KvPair, 0),
	}
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	n := req.Limit
	for iter.Seek(req.GetStartKey()); iter.Valid() && n > 0; iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})
		n--
	}

	return resp, nil
}
