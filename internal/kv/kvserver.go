package kv

import (
	"TicketX/internal/persister"
	"TicketX/internal/raft"
	"TicketX/proto"
	"context"
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
	mu sync.Mutex
	kv map[string]Value
	proto.UnimplementedKvServer
	applyCh chan raft.ApplyMsg    //和raft通信的管道
	waitCh  map[int64]chan result //确保put请求成功commit的管道

	getCh map[int64]chan result //暂时确保get一致性，多机删

	lastRequest map[int64]int64 //请求者对应的最后一个请求编号
	rf          *raft.Raft
	lastApplied int64
	lastResult  map[int]result //上一次请求的结果
}

type Value struct {
	Value    string
	Version  int64
	ExpireAt int64
}

func (kv *KvServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetReply, error) {

	_, ok := kv.rf.GetState()
	//快速读
	if ok && (time.Since(kv.rf.LastHeartbeat()) < 50*time.Millisecond) {
		if int(kv.lastApplied) < kv.rf.GetCommitIndex() {
			val := kv.kv[req.Key]
			return &proto.GetReply{
				Error:   proto.ErrorType_OK,
				Value:   val.Value,
				Version: val.Version,
			}, nil
		}
	}

	//慢速读
	op := &proto.Op{
		Type:      "Get",
		Key:       req.Key,
		ClientId:  req.ClientId,
		RequestId: req.RequestId,
	}
	data, _ := po.Marshal(op)
	index, _, isleader, leader := kv.rf.Start(data)
	if !isleader {
		return &proto.GetReply{Error: proto.ErrorType_WRONG_LEADER, LeaderId: leader}, nil
	}

	ch := make(chan result, 1)

	kv.mu.Lock()
	kv.getCh[int64(index)] = ch
	kv.mu.Unlock()

	res := <-ch

	return &proto.GetReply{
		Error:    res.Err,
		Value:    res.Value,
		Version:  res.Version,
		LeaderId: leader,
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

// func (kv *KvServer) ttlWorker() {
//     for {
//         time.Sleep(time.Second)

//         now := time.Now().Unix()

//         for k, v := range kv.kv {
//             if v.ExpireAt != 0 && now > v.ExpireAt {
//                 // delete(kv.kv, k)走Raft
//             }
//         }
//     }
// }

func MakeKVServer(peers []string, me int) *KvServer {
	applych := make(chan raft.ApplyMsg)
	persister := persister.MakePersister()

	kv := &KvServer{}

	kv.kv = make(map[string]Value)
	kv.applyCh = applych
	kv.rf = raft.MakeRaft(applych, peers, int32(me), persister)
	kv.waitCh = make(map[int64]chan result)
	kv.lastRequest = make(map[int64]int64)
	kv.getCh = make(map[int64]chan result)

	go kv.applyLoop() //循环执行命令
	//go kv.ttlWorker()//删除过期建
	return kv
}
