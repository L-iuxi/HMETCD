package watch

import (
	"fmt"
	"sync"
)

type WatchEvent struct {
	Key      string
	Value    string
	Type     string
	Revision int64
}
type Watcher struct {
	Key string
	Id  int64
	Ch  chan WatchEvent
}
type WatcherManager struct {
	mu            sync.RWMutex
	nextWatcherId int64
	watchers      map[string]map[int64]*Watcher
}

func NewWatcherManager() *WatcherManager {
	return &WatcherManager{
		nextWatcherId: 1,
		watchers:      make(map[string]map[int64]*Watcher),
	}
}

// 添加到监听表中
func (wm *WatcherManager) AddWatcher(key string) *Watcher {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	wm.nextWatcherId++

	w := &Watcher{
		Key: key,
		Id:  wm.nextWatcherId,
		Ch:  make(chan WatchEvent, 100),
	}

	if wm.watchers[key] == nil {
		wm.watchers[key] = make(map[int64]*Watcher)
	}
	wm.watchers[key][w.Id] = w
	return w
}

func (wm *WatcherManager) Notify(event WatchEvent) {

	wm.mu.RLock()

	watchers := wm.watchers[event.Key]

	wm.mu.RUnlock()

	fmt.Println("notify watcher:", event.Key)

	for _, w := range watchers {

		select {

		case w.Ch <- event:

			fmt.Println("send event to watcher")

		default:

			fmt.Println("watcher channel full")
		}
	}
}
