package data_struct

import (
    "github.com/emirpasic/gods/maps/treemap"
    "github.com/emirpasic/gods/utils"
    "sync"
)

type ConcurrentTreeMap struct {
    mutex sync.RWMutex
    tm *treemap.Map
}

func NewConcurrentTreeMap(comparator utils.Comparator) *ConcurrentTreeMap {
    return &ConcurrentTreeMap{
        tm: treemap.NewWith(comparator),
    }
}

func (ctm *ConcurrentTreeMap) Put(key interface{}, val interface{}) {
    ctm.mutex.Lock()
    defer ctm.mutex.Unlock()
    ctm.tm.Put(key, val)
}

func (ctm *ConcurrentTreeMap) Remove(key interface{}) {
    ctm.mutex.Lock()
    defer ctm.mutex.Unlock()
    ctm.tm.Remove(key)
}

func (ctm *ConcurrentTreeMap) Max() (interface{}, interface{}) {
    ctm.mutex.RLock()
    defer ctm.mutex.RUnlock()
    return ctm.tm.Max()
}
