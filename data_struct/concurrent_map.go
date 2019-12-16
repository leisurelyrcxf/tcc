package data_struct

import (
	"hash/crc32"
	"sync"
)

type concurrentMapPartition struct {
	mutex sync.RWMutex
	m     map[string]interface{}
}

func (cmp *concurrentMapPartition) get(key string) (interface{}, bool) {
	cmp.mutex.RLock()
	defer cmp.mutex.RUnlock()
	val, ok := cmp.m[key]
	return val, ok
}

func (cmp *concurrentMapPartition) set(key string, val interface{}) {
	cmp.mutex.Lock()
	defer cmp.mutex.Unlock()
	cmp.m[key] = val
}

func (cmp *concurrentMapPartition) del(key string) {
	cmp.mutex.Lock()
	defer cmp.mutex.Unlock()
	delete(cmp.m, key)
}

func (cmp *concurrentMapPartition) forEachLocked(cb func(string, interface{})) {
	for key, val := range cmp.m {
		cb(key, val)
	}
}

type ConcurrentMap struct {
	partitions []concurrentMapPartition
}

func NewConcurrentMap(partitionNum int) ConcurrentMap {
	cm := ConcurrentMap{partitions: make([]concurrentMapPartition, partitionNum)}
	for i := 0; i < partitionNum; i++ {
		cm.partitions[i].m = make(map[string]interface{})
	}
	return cm
}

func (cmp *ConcurrentMap) hash(s string) int {
	return int(crc32.ChecksumIEEE([]byte(s))) % len(cmp.partitions)
}

func (cmp *ConcurrentMap) RLock() {
	for i := 0; i < len(cmp.partitions); i++ {
		cmp.partitions[i].mutex.RLock()
	}
}

func (cmp *ConcurrentMap) RUnlock() {
	for i := len(cmp.partitions) - 1; i >= 0; i-- {
		cmp.partitions[i].mutex.RUnlock()
	}
}

func (cmp *ConcurrentMap) Get(key string) (interface{}, bool) {
	return cmp.partitions[cmp.hash(key)].get(key)
}

func (cmp *ConcurrentMap) Set(key string, val interface{}) {
	cmp.partitions[cmp.hash(key)].set(key, val)
}

func (cmp *ConcurrentMap) Del(key string) {
	cmp.partitions[cmp.hash(key)].del(key)
}

func (cmp *ConcurrentMap) ForEachLoosed(cb func(string, interface{})) {
	for _, partition := range cmp.partitions {
		partition.mutex.RLock()
		partition.forEachLocked(cb)
		partition.mutex.RUnlock()
	}
}

func (cmp *ConcurrentMap) ForEachStrict(cb func(string, interface{})) {
	cmp.RLock()
	for _, partition := range cmp.partitions {
		partition.forEachLocked(cb)
	}
	cmp.RUnlock()
}

func (cmp *ConcurrentMap) Size() (sz int) {
	cmp.RLock()
	for _, partition := range cmp.partitions {
		sz += len(partition.m)
	}
	cmp.RUnlock()
	return
}
