package data_struct

import (
	"hash/crc32"
	"sync"
)

type concurrentMapPartition struct {
	mutex sync.RWMutex
	m     map[string]float64
}

func (cmp *concurrentMapPartition) get(key string) (float64, bool) {
	cmp.mutex.RLock()
	defer cmp.mutex.RUnlock()
	val, ok := cmp.m[key]
	return val, ok
}

func (cmp *concurrentMapPartition) set(key string, val float64) {
	cmp.mutex.Lock()
	defer cmp.mutex.Unlock()
	cmp.m[key] = val
}

func (cmp *concurrentMapPartition) del(key string) {
	cmp.mutex.Lock()
	defer cmp.mutex.Unlock()
	delete(cmp.m, key)
}

type ConcurrentMap struct {
	partitions []concurrentMapPartition
}

func NewConcurrentMap(partitionNum int) ConcurrentMap {
	cm := ConcurrentMap{partitions: make([]concurrentMapPartition, partitionNum)}
	for i := 0; i < partitionNum; i++ {
		cm.partitions[i].m = make(map[string]float64)
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

func (cmp *ConcurrentMap) Get(key string) (float64, bool) {
	return cmp.partitions[cmp.hash(key)].get(key)
}

func (cmp *ConcurrentMap) Set(key string, val float64) {
	cmp.partitions[cmp.hash(key)].set(key, val)
}

func (cmp *ConcurrentMap) Del(key string) {
	cmp.partitions[cmp.hash(key)].del(key)
}

func (cmp *ConcurrentMap) ForEach(cb func(string, float64)) {
	cmp.RLock()
	for _, partition := range cmp.partitions {
		for key, val := range partition.m {
			cb(key, val)
		}
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
