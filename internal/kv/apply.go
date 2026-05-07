package kv

import (
	types "TicketX/internal/type"
	"TicketX/proto"
	"fmt"
	"time"

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
				now := time.Now().Unix()
				var Err proto.ErrorType
				var v types.Value
				var ok bool

				if op.RequestId <= last { //重复请求不重复执行只返回
					Err = proto.ErrorType_OK
					if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
						ch <- kv.lastResult[int(op.ClientId)]
					}
					delete(kv.waitCh, msg.CommandIndex)
					continue
				} else { //未重复请求执行并记录

					v, _ = kv.store.Get(op.Key)

					if op.ExpectedVersion != 0 { //检查version

						if !ok || v.Version != op.ExpectedVersion { //版本不匹配
							res := result{Err: proto.ErrorType_WRONG_VERSION, Version: v.Version}
							kv.lastResult[int(op.ClientId)] = res
							kv.lastRequest[op.ClientId] = op.RequestId
							if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
								ch <- res
								delete(kv.waitCh, msg.CommandIndex)
							}
							continue

						} else {
							v.Value = op.Value
							v.Version = v.Version + 1

							kv.store.Put(op.Key, v)

							kv.lastRequest[op.ClientId] = op.RequestId

							Err = proto.ErrorType_OK
							kv.lastResult[int(op.ClientId)] = result{
								Err:     Err,
								Version: v.Version,
							}
						}
					} else {
						// 第一次请求
						if !ok {
							v = types.Value{Value: op.Value, Version: 1}
						} else {
							v.Value = op.Value
							v.Version++
						}
						if op.TTL > 0 {
							leaseID := kv.leaseMgr.BindOrRefresh(op.Key, op.TTL, now)
							v.ExpireAt = now + op.TTL
							_ = leaseID
						}
						kv.store.Put(op.Key, v)
						kv.lastRequest[op.ClientId] = op.RequestId

						Err = proto.ErrorType_OK
						kv.lastResult[int(op.ClientId)] = result{
							Err:     Err,
							Version: v.Version,
						}
					}
				}
				//返回结果给put
				if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
					ch <- result{
						Err:     Err,
						Version: v.Version,
					}
					delete(kv.waitCh, msg.CommandIndex)
				}

			case "Get":
				if ch, ok := kv.getCh[msg.CommandIndex]; ok {
					if v, exists := kv.store.Get(op.Key); exists != nil {
						ch <- result{Value: v.Value, Version: v.Version, Err: proto.ErrorType_OK}
					} else {
						ch <- result{Err: proto.ErrorType_KEY_NOT_EXIST}
					}
					delete(kv.getCh, msg.CommandIndex)
				}
			case "Expire":
				kv.expireByKey(op.Key)

			}

			kv.mu.Unlock()
		}

	}
}
func (kv *KvServer) expireByKey(key string) {
	v, err := kv.store.Get(key)
	if err != nil {
		return
	}
	if v.ExpireAt == 0 || v.ExpireAt > time.Now().Unix() {
		return
	}
	kv.store.Delete(key)
	_ = kv.leaseMgr.RemoveKey(key)
}
