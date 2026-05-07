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
				kv.currentRev++
				rev := kv.currentRev

				// 去重
				last := kv.lastRequest[op.ClientId]
				if op.RequestId <= last {
					if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
						ch <- kv.lastResult[int(op.ClientId)]
					}
					delete(kv.waitCh, msg.CommandIndex)
					continue
				}

				// version check（基于 latest）
				if op.ExpectedVersion != 0 {
					latestRev := kv.latest[op.Key]
					if latestRev != op.ExpectedVersion {
						res := result{
							Err:     proto.ErrorType_WRONG_VERSION,
							Version: latestRev,
						}
						kv.lastResult[int(op.ClientId)] = res

						if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
							ch <- res
						}
						delete(kv.waitCh, msg.CommandIndex)
						continue
					}
				}
				// 写 MVCC 数据
				v := types.Value{
					Value: op.Value,
				}

				key := fmt.Sprintf("%s/%d", op.Key, rev)
				kv.store.Put(key, v)

				kv.latest[op.Key] = rev

				kv.lastRequest[op.ClientId] = op.RequestId

				res := result{
					Err:     proto.ErrorType_OK,
					Version: rev,
				}
				kv.lastResult[int(op.ClientId)] = res

				if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
					ch <- res
					delete(kv.waitCh, msg.CommandIndex)
				}

			case "Get":
				if ch, ok := kv.getCh[msg.CommandIndex]; ok {
					rev, ok := kv.latest[op.Key]
					if !ok {
						ch <- result{Err: proto.ErrorType_KEY_NOT_EXIST}
						delete(kv.getCh, msg.CommandIndex)
						continue
					}

					key := fmt.Sprintf("%s/%d", op.Key, rev)

					v, err := kv.store.Get(key)
					if err != nil {
						ch <- result{Err: proto.ErrorType_KEY_NOT_EXIST}
					} else {
						ch <- result{
							Value: v.Value,
							Err:   proto.ErrorType_OK,
						}
					}

					delete(kv.getCh, msg.CommandIndex)
				}
			// case "Expire":
			// 	kv.expireByKey(op.Key)
			case "Delete":
				kv.currentRev++
				rev := kv.currentRev

				v := types.Value{
					Value:   "",
					Deleted: true,
				}

				key := fmt.Sprintf("%s/%d", op.Key, rev)
				kv.store.Put(key, v)
				kv.latest[op.key] = rev

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
