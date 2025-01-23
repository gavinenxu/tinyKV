package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).

	resp := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		return resp, err
	}

	txn := mvcc.NewMvccTxn(reader, req.GetVersion())

	// 1. acquire lock and check whether it's locked by other txn
	lock, err := txn.GetLock(req.GetKey())
	if err != nil {
		return resp, err
	}
	if lock != nil && lock.Ts <= req.GetVersion() {
		// we have a more broad lock range
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.GetKey(),
				LockTtl:     lock.Ttl,
			},
		}
		return resp, nil
	}

	// 2. parse value
	val, err := txn.GetValue(req.GetKey())
	if err != nil {
		return resp, err
	}
	if val == nil {
		resp.NotFound = true
		return resp, nil
	}

	resp.Value = val
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		return resp, err
	}

	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	errors := make([]*kvrpcpb.KeyError, 0)
	for _, mut := range req.GetMutations() {

		// 1. check whether we have other write ongoing
		write, ts, err := txn.MostRecentWrite(mut.GetKey())
		if err != nil {
			return resp, err
		}
		if write != nil && ts >= req.GetStartVersion() {
			// if the previous write is more latest than current changes, we find the write conflict
			errors = append(errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.GetStartVersion(),
					ConflictTs: ts,
					Key:        mut.GetKey(),
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}

		// 2. check whether this transaction is locked by other txn
		lock, err := txn.GetLock(mut.GetKey())
		if err != nil {
			return resp, err
		}
		if lock != nil && lock.Ts <= req.GetStartVersion() {
			// we have a more broad lock range
			errors = append(errors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					LockTtl:     lock.Ttl,
					Key:         mut.GetKey(),
				},
			})
			continue
		}

		// 3. write lock and value for each of mutation in transaction
		txn.PutLock(mut.GetKey(), &mvcc.Lock{
			Primary: req.GetPrimaryLock(),
			Ts:      req.GetStartVersion(),
			Ttl:     req.GetLockTtl(),
			Kind:    getWriteKind(mut.Op),
		})
		txn.PutValue(mut.GetKey(), mut.GetValue())
	}

	resp.Errors = errors
	if len(resp.Errors) > 0 {
		return resp, nil
	}

	// flush all transaction changes to storage at a time
	// If the item has the same key, it will be replaced by the last item
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return resp, err
	}

	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).

	resp := &kvrpcpb.CommitResponse{}
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		return resp, err
	}

	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	server.Latches.WaitForLatches(req.GetKeys())
	defer server.Latches.ReleaseLatches(req.GetKeys())

	for _, key := range req.Keys {
		// 1. check whether write has been committed for this transaction
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		if write != nil && write.Kind != mvcc.WriteKindRollback && write.StartTS == req.GetStartVersion() {
			// write is the same time as current transaction and write is not rollback
			return resp, nil
		}

		// 2. check lock
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock == nil || lock.Ts != req.GetStartVersion() {
			// if lock is null or which is the different transaction, we should abort
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
			}
			return resp, nil
		}

		// 3. delete lock and commit write
		txn.DeleteLock(key)
		txn.PutWrite(key, req.GetCommitVersion(), &mvcc.Write{
			StartTS: req.GetStartVersion(),
			Kind:    lock.Kind,
		})
	}

	// flush to storage
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return resp, err
	}

	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).

	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		return resp, err
	}

	pairs := make([]*kvrpcpb.KvPair, 0)

	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	scanner := mvcc.NewScanner(req.GetStartKey(), txn)
	defer scanner.Close()

	n := req.GetLimit()
	for n > 0 {
		key, val, err := scanner.Next()
		if err != nil {
			return resp, err
		}
		// no more data, break loop
		if key == nil {
			break
		}

		// add not deleted value
		if val != nil {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Key:   key,
				Value: val,
			})
		}
		n--
	}

	resp.Pairs = pairs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).

	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		return resp, err
	}

	txn := mvcc.NewMvccTxn(reader, req.GetLockTs())

	write, ts, err := txn.CurrentWrite(req.GetPrimaryKey())
	if err != nil {
		return resp, err
	}
	// record has been committed, don't need to rollback
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		resp.CommitVersion = ts
		resp.Action = kvrpcpb.Action_NoAction
		return resp, nil
	}

	lock, err := txn.GetLock(req.GetPrimaryKey())
	if err != nil {
		return resp, err
	}

	if lock == nil {
		// The lock does not exist, put a rollback record
		txn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		})

		resp.Action = kvrpcpb.Action_LockNotExistRollback
	} else if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.GetCurrentTs()) {
		// lock expire to rollback

		// delete default and lock
		txn.DeleteValue(req.GetPrimaryKey())
		txn.DeleteLock(req.GetPrimaryKey())
		// put a rollback write record
		txn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		})

		resp.Action = kvrpcpb.Action_TTLExpireRollback
	} else {
		resp.Action = kvrpcpb.Action_NoAction
	}

	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return resp, err
	}

	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		return resp, err
	}

	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	server.Latches.WaitForLatches(req.GetKeys())
	defer server.Latches.ReleaseLatches(req.GetKeys())

	seen := make(map[string]bool)
	for _, key := range req.GetKeys() {
		if _, ok := seen[string(key)]; ok {
			continue
		}
		seen[string(key)] = true

		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				// committed write
				resp.Error = &kvrpcpb.KeyError{
					Abort: "Already committed",
				}
			}
			// already rollback, do nothing
			continue
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}

		if lock == nil || lock.Ts != req.GetStartVersion() {
			// rollback an empty or wrong transaction
			txn.PutWrite(key, req.GetStartVersion(), &mvcc.Write{
				StartTS: req.GetStartVersion(),
				Kind:    mvcc.WriteKindRollback,
			})
		} else {
			txn.DeleteValue(key)
			txn.DeleteLock(key)
			txn.PutWrite(key, req.GetStartVersion(), &mvcc.Write{
				StartTS: req.GetStartVersion(),
				Kind:    mvcc.WriteKindRollback,
			})
		}
	}

	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return resp, err
	}

	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		return resp, err
	}

	// use iter to get all lock info
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()

	// parse lock to get available resolve keys based on start version
	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return resp, err
		}
		if lock != nil && lock.Ts == req.GetStartVersion() {
			keys = append(keys, item.KeyCopy(nil))
		}
	}

	// rollback all the keys
	if req.GetCommitVersion() == 0 {
		rollbackResp, err := server.KvBatchRollback(context.Background(), &kvrpcpb.BatchRollbackRequest{
			Keys:         keys,
			StartVersion: req.GetStartVersion(),
		})
		if err != nil {
			return resp, err
		}
		resp.Error = rollbackResp.Error
	} else {
		// commit all the possible keys
		commitResp, err := server.KvCommit(context.Background(), &kvrpcpb.CommitRequest{
			Keys:          keys,
			StartVersion:  req.GetStartVersion(),
			CommitVersion: req.GetCommitVersion(),
		})
		if err != nil {
			return resp, err
		}
		resp.Error = commitResp.Error
	}

	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}

func getWriteKind(op kvrpcpb.Op) mvcc.WriteKind {
	switch op {
	case kvrpcpb.Op_Put:
		return mvcc.WriteKindPut
	case kvrpcpb.Op_Del:
		return mvcc.WriteKindDelete
	case kvrpcpb.Op_Rollback:
		return mvcc.WriteKindRollback
	default:
		panic("unknown op")
	}
}
