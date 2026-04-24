package kv

import (
	"TicketX/proto"
	"fmt"

	po "google.golang.org/protobuf/proto"
)

type Order struct {
	OrderID  string
	UserID   string
	TicketID string
	UniqueID int64
	Status   string
	Time     int64
}

// 接受管道来到msg并执行
func (kv *KvServer) applyLoop() {

	for msg := range kv.applyCh {
		kv.lastApplied = msg.CommandIndex
		if msg.CommandValid {
			data := msg.Command.([]byte)
			var op proto.Op
			err := po.Unmarshal(data, &op)
			if err != nil {
				fmt.Println("unmarshal error:", err)
				return
			}

			kv.mu.Lock()

			switch op.Type {
			case "Put":
				last := kv.lastRequest[op.ClientId]
				if op.RequestId <= last { //重复请求不重复执行只返回

				} else { //未重复请求执行并记录
					kv.kv[op.Key] = op.Value
					kv.lastRequest[op.ClientId] = op.RequestId
				}
				//返回结果给put
				if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
					ch <- result{Err: proto.ErrorType_OK}
					delete(kv.waitCh, msg.CommandIndex)
				}
			case "Get": //不在乎重复请求
				if ch, ok := kv.getCh[msg.CommandIndex]; ok {
					ch <- result{Value: kv.kv[op.Key], Err: proto.ErrorType_OK}
					delete(kv.getCh, msg.CommandIndex)
				} else {
					ch <- result{Value: kv.kv[op.Key], Err: proto.ErrorType_KEY_NOT_EXIST}
					delete(kv.getCh, msg.CommandIndex)
				}

			}

			kv.mu.Unlock()
		}

	}
}
