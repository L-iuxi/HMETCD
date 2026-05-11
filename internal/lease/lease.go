package lease

import (
	"errors"
	"sync"
	"time"
)

var errLeaseNotFound = errors.New("lease not found")

type Lease struct {
	ID        int64
	TTL       int64 // second
	ExpiresAt int64 // unix second
	Key       string
}

type LeaseManager struct {
	sync.RWMutex
	leases      map[int64]*Lease
	keyToLease  map[string]int64
	nextLeaseID int64
	minLeaseTTL int64
}

func NewLeaseManager(minLeaseTTL time.Duration) *LeaseManager {
	minTTL := int64(minLeaseTTL.Seconds())
	if minTTL <= 0 {
		minTTL = 1
	}
	return &LeaseManager{
		leases:      make(map[int64]*Lease),
		keyToLease:  make(map[string]int64),
		nextLeaseID: 1,
		minLeaseTTL: minTTL,
	}
}

// BindOrRefresh 实现 1:1 key->lease。
func (lm *LeaseManager) BindOrRefresh(key string, ttl, now int64) int64 {
	lm.Lock()
	defer lm.Unlock()

	if ttl < lm.minLeaseTTL {
		ttl = lm.minLeaseTTL
	}

	if id, ok := lm.keyToLease[key]; ok {
		l := lm.leases[id]
		l.TTL = ttl
		l.ExpiresAt = now + ttl
		return id
	}

	id := lm.nextLeaseID
	lm.nextLeaseID++
	lm.leases[id] = &Lease{ID: id, TTL: ttl, ExpiresAt: now + ttl, Key: key}
	lm.keyToLease[key] = id
	return id
}

func (lm *LeaseManager) ExpiredKeys(now int64) []string {
	lm.RLock()
	defer lm.RUnlock()
	ret := make([]string, 0)
	for _, lease := range lm.leases {
		if lease.ExpiresAt <= now {
			ret = append(ret, lease.Key)
		}
	}
	return ret
}

func (lm *LeaseManager) RemoveKey(key string) error {
	lm.Lock()
	defer lm.Unlock()
	id, ok := lm.keyToLease[key]
	if !ok {
		return errLeaseNotFound
	}
	delete(lm.keyToLease, key)
	delete(lm.leases, id)
	return nil
}

func (lm *LeaseManager) KeepAliveByKey(key string, now int64) error {
	lm.Lock()
	defer lm.Unlock()
	id, ok := lm.keyToLease[key]
	if !ok {
		return errLeaseNotFound
	}
	l := lm.leases[id]
	l.ExpiresAt = now + l.TTL
	return nil
}
