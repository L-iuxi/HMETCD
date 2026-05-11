package kv

import (
	"TicketX/internal/db"
	"TicketX/internal/lease"
	"TicketX/internal/persister"
	"TicketX/internal/raft"
	"TicketX/internal/watch"
	"TicketX/proto"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	po "google.golang.org/protobuf/proto"
)

// 把applyloop结果返回给put/get的
type result struct {
	Value   string
	Version int64
	Err     proto.ErrorType
}

type KvServer struct {
	mu         sync.Mutex
	store      *db.Store
	currentRev int64
	proto.UnimplementedKvServer
	applyCh chan raft.ApplyMsg    //和raft通信的管道
	waitCh  map[int64]chan result //确保put请求成功commit的管道

	getCh map[int64]chan result //暂时确保get一致性，多机删

	lastRequest map[int64]int64 //请求者对应的最后一个请求编号
	rf          *raft.Raft
	lastApplied int64
	lastResult  map[int]result //上一次请求的结果
	leaseMgr    *lease.LeaseManager
	latest      map[string]int64   //每个建的最新版本
	history     map[string][]int64 //每个建的历史版本

	watcherManager *watch.WatcherManager
	eventNotifier  chan watch.WatchEvent
}

func (kv *KvServer) Watch(req *proto.WatchRequest, stream proto.Kv_WatchServer) error {

	fmt.Println("watch rpc enter")

	watcher :=
		kv.watcherManager.AddWatcher(req.Key)

	for {

		select {

		case ev := <-watcher.Ch:

			fmt.Println("grpc send event")

			err := stream.Send(
				&proto.WatchResponse{
					Key:      ev.Key,
					Value:    ev.Value,
					Revision: ev.Revision,
					Type:     ev.Type,
					Err:      proto.ErrorType_OK,
				},
			)

			if err != nil {

				fmt.Println(
					"stream send error:",
					err,
				)

				return err
			}
		}
	}
}
func (kv *KvServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetReply, error) {

	_, ok := kv.rf.GetState()

	//快速读
	if ok && time.Since(kv.rf.LastHeartbeat()) < 50*time.Millisecond {
		if int(kv.lastApplied) < kv.rf.GetCommitIndex() {

			val, err := kv.store.Get(req.Key)
			if err != nil {
				return &proto.GetReply{
					Error: proto.ErrorType_KEY_NOT_EXIST,
				}, nil
			}

			return &proto.GetReply{
				Error:   proto.ErrorType_OK,
				Value:   val.Value,
				Version: val.Version,
			}, nil
		}
	}

	op := &proto.Op{
		Type:            "Get",
		Key:             req.Key,
		ExpectedVersion: req.Version, // MVCC revision
		ClientId:        req.ClientId,
		RequestId:       req.RequestId,
	}

	data, _ := po.Marshal(op)

	index, _, isleader, leader := kv.rf.Start(data)
	if !isleader {
		return &proto.GetReply{
			Error:    proto.ErrorType_WRONG_LEADER,
			LeaderId: leader,
		}, nil
	}

	ch := make(chan result, 1)

	kv.mu.Lock()
	kv.getCh[int64(index)] = ch
	kv.mu.Unlock()

	res := <-ch

	return &proto.GetReply{
		Error:   res.Err,
		Value:   res.Value,
		Version: res.Version,
	}, nil
}
func (kv *KvServer) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutReply, error) {

	op := &proto.Op{
		Type:            "Put",
		Key:             req.Key,
		Value:           req.Value,
		ExpectedVersion: req.ExpectedVersion,
		TTL:             req.Ttl,
		ClientId:        req.ClientId,
		RequestId:       req.RequestId,
	}
	data, _ := po.Marshal(op)
	index, _, isleader, leader := kv.rf.Start(data)
	if !isleader {
		return &proto.PutReply{
			Error:    proto.ErrorType_WRONG_LEADER,
			LeaderId: leader}, nil
	}

	ch := make(chan result, 1)

	kv.mu.Lock()
	kv.waitCh[int64(index)] = ch
	kv.mu.Unlock()

	res := <-ch

	return &proto.PutReply{
		Error:    res.Err,
		Version:  res.Version,
		LeaderId: leader,
	}, nil
}

func (kv *KvServer) GetRaft() *raft.Raft {
	return kv.rf
}

// 仅 leader 扫描，发现到期后通过 Raft 提交 Expire 命令。
func (kv *KvServer) leaseExpireWorker() {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}
		now := time.Now().Unix()
		expiredKeys := kv.leaseMgr.ExpiredKeys(now)

		for _, key := range expiredKeys {
			op := &proto.Op{Type: "Expire", Key: key}
			data, _ := po.Marshal(op)
			kv.rf.Start(data)
		}
	}
}

func MakeKVServer(peers []string, me int) *KvServer {
	applych := make(chan raft.ApplyMsg)
	persister := persister.MakePersister()

	kv := &KvServer{}
	path := fmt.Sprintf("./data/node-%d", me)
	os.MkdirAll(path, 0755)

	store, err := db.NewStore(path)
	if err != nil {
		panic(err)
	}
	kv.store = store
	kv.applyCh = applych
	kv.rf = raft.MakeRaft(applych, peers, int32(me), persister)
	kv.waitCh = make(map[int64]chan result)
	kv.lastRequest = make(map[int64]int64)
	kv.getCh = make(map[int64]chan result)
	kv.lastResult = make(map[int]result)
	kv.history = make(map[string][]int64)
	kv.latest = make(map[string]int64)
	kv.leaseMgr = lease.NewLeaseManager(1 * time.Second)

	kv.watcherManager = watch.NewWatcherManager()
	go kv.applyLoop() //循环执行命令
	go kv.leaseExpireWorker()

	return kv
}
