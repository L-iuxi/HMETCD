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
	Value string
	Err   proto.ErrorType
}

type KvServer struct {
	mu sync.Mutex
	kv map[string]string
	proto.UnimplementedKvServer
	applyCh chan raft.ApplyMsg    //和raft通信的管道
	waitCh  map[int64]chan result //确保put请求成功commit的管道

	getCh map[int64]chan result //暂时确保get一致性，多机删

	lastRequest map[int64]int64 //请求者对应的最后一个请求编号
	rf          *raft.Raft
	lastApplied int64
}

type OpType string

const (
	Put    OpType = "Put"
	Get    OpType = "Get"
	Delete OpType = "Delete"
)

func (kv *KvServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetReply, error) {

	_, ok := kv.rf.GetState()
	if ok && (time.Since(kv.rf.LastHeartbeat()) < 50*time.Millisecond) {
		if int(kv.lastApplied) < kv.rf.GetCommitIndex() {
			return &proto.GetReply{
				Error: proto.ErrorType_OK,
				Value: kv.kv[req.Key],
			}, nil
		}
	}

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
		Error: res.Err,
		Value: res.Value,
	}, nil
}

func (kv *KvServer) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutReply, error) {

	op := &proto.Op{
		Type:      "Put",
		Key:       req.Key,
		Value:     req.Value,
		ClientId:  req.ClientId,
		RequestId: req.RequestId,
	}
	data, _ := po.Marshal(op)
	index, _, isleader, leader := kv.rf.Start(data)
	if !isleader {
		return &proto.PutReply{Error: proto.ErrorType_WRONG_LEADER, LeaderId: leader}, nil
	}

	ch := make(chan result, 1)

	kv.mu.Lock()
	kv.waitCh[int64(index)] = ch
	kv.mu.Unlock()

	res := <-ch

	return &proto.PutReply{
		Error: res.Err,
	}, nil
}

func (kv *KvServer) GetRaft() *raft.Raft {
	return kv.rf
}

func MakeKVServer(peers []string, me int) *KvServer {
	applych := make(chan raft.ApplyMsg)
	persister := persister.MakePersister()

	kv := &KvServer{}

	kv.kv = make(map[string]string)
	kv.applyCh = applych
	kv.rf = raft.MakeRaft(applych, peers, int32(me), persister)
	kv.waitCh = make(map[int64]chan result)
	kv.lastRequest = make(map[int64]int64)
	kv.getCh = make(map[int64]chan result)

	go kv.applyLoop() //循环执行命令

	return kv
}
