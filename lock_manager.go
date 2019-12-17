package ts_promote

import (
    "hash/crc32"
    "sync"
)

type LockManager struct {
    mutexes []sync.RWMutex
}

func NewLockManager() *LockManager {
    return &LockManager{mutexes:make([]sync.RWMutex, 1024)}
}

func (lm *LockManager) hash(key string) int {
    return int(crc32.ChecksumIEEE([]byte(key))) % len(lm.mutexes)
}

func (lm *LockManager) Lock(key string) {
    lm.mutexes[lm.hash(key)].Lock()
}

func (lm *LockManager) Unlock(key string) {
    lm.mutexes[lm.hash(key)].Unlock()
}

func (lm *LockManager) RLock(key string) {
    lm.mutexes[lm.hash(key)].RLock()
}

func (lm *LockManager) RUnlock(key string) {
    lm.mutexes[lm.hash(key)].RUnlock()
}

