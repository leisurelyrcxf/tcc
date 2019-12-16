package ts_promote

import (
    "hash/crc32"
    "sync"
)

type LockManager struct {
    mutexes []sync.Mutex
}

func NewLockManager() *LockManager {
    return &LockManager{mutexes:make([]sync.Mutex, 1024)}
}

func (lm *LockManager) hash(key string) int {
    return int(crc32.ChecksumIEEE([]byte(key))) % len(lm.mutexes)
}

func (lm *LockManager) lockKey(key string) {
    lm.mutexes[lm.hash(key)].Lock()
}

func (lm *LockManager) unlockKey(key string) {
    lm.mutexes[lm.hash(key)].Unlock()
}

