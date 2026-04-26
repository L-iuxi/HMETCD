package kv

import (
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
				var v Value
				var ok bool

				if op.RequestId <= last { //重复请求不重复执行只返回
					Err = proto.ErrorType_OK
					if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
						ch <- kv.lastResult[int(op.ClientId)]
					}
					delete(kv.waitCh, msg.CommandIndex)
					continue
				} else { //未重复请求执行并记录

					v, ok = kv.kv[op.Key]

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

							kv.kv[op.Key] = v

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
							v = Value{Value: op.Value, Version: 1}
						} else {
							v.Value = op.Value
							v.Version++
						}
						if op.TTL > 0 {
							v.ExpireAt = now + op.TTL
						}
						kv.kv[op.Key] = v
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

			case "Get": //不在乎重复请求
				if ch, ok := kv.getCh[msg.CommandIndex]; ok {
					ch <- result{
						Value:   kv.kv[op.Key].Value,
						Err:     proto.ErrorType_OK,
						Version: kv.kv[op.Key].Version,
					}
					delete(kv.getCh, msg.CommandIndex)
				} else {
					ch <- result{
						Value:   kv.kv[op.Key].Value,
						Err:     proto.ErrorType_KEY_NOT_EXIST,
						Version: kv.kv[op.Key].Version,
					}
					delete(kv.getCh, msg.CommandIndex)
				}

			}

			kv.mu.Unlock()
		}

	}
}
