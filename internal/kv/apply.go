package kv

import (
	types "TicketX/internal/type"
	"TicketX/internal/watch"
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

				kv.latest[op.Key] = rev //记录该建最后一个版本

				kv.lastRequest[op.ClientId] = op.RequestId //记录该clientid最后一个请求结果

				kv.history[key] = append(kv.history[key], rev) //记录该版本下该建值
				kv.watcherManager.Notify(watch.WatchEvent{
					Type:     "Put",
					Key:      op.Key,
					Value:    op.Value,
					Revision: rev,
				})
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

					key := op.Key
					version := op.ExpectedVersion // 客户端传来的 version

					var rev int64
					var ok2 bool

					// 最新读

					if version == 0 {
						rev, ok2 = kv.latest[key]
						if !ok2 {
							ch <- result{Err: proto.ErrorType_KEY_NOT_EXIST}
							delete(kv.getCh, msg.CommandIndex)
							continue
						}
					} else {

						// 历史版本读

						revs, ok2 := kv.history[key]
						if !ok2 {
							ch <- result{Err: proto.ErrorType_KEY_NOT_EXIST}
							delete(kv.getCh, msg.CommandIndex)
							continue
						}

						rev = findLE(revs, version)
						if rev == 0 {
							ch <- result{Err: proto.ErrorType_KEY_NOT_EXIST}
							delete(kv.getCh, msg.CommandIndex)
							continue
						}
					}

					// 去 store 查
					realKey := fmt.Sprintf("%s/%d", key, rev)

					v, err := kv.store.Get(realKey)
					if err != nil || v.Deleted {
						ch <- result{Err: proto.ErrorType_KEY_NOT_EXIST}
					} else {
						ch <- result{
							Value:   v.Value,
							Version: v.Version,
							Err:     proto.ErrorType_OK,
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
				kv.latest[key] = rev                           //记录该建最后一个版本
				kv.history[key] = append(kv.history[key], rev) //记录该版本下该建值
				kv.watcherManager.Notify(watch.WatchEvent{
					Type:     "Put",
					Key:      op.Key,
					Value:    op.Value,
					Revision: rev,
				})
			}

			kv.mu.Unlock()
		}

	}
}
func findLE(revs []int64, target int64) int64 {
	var res int64 = 0

	for _, r := range revs {
		if r <= target {
			res = r
		} else {
			break
		}
	}

	return res
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
