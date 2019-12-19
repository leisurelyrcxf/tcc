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

func (ctm *ConcurrentTreeMap) Get(key interface{}) (interface{}, bool) {
    ctm.mutex.Lock()
    defer ctm.mutex.Unlock()
    return ctm.tm.Get(key)
}

func (ctm *ConcurrentTreeMap) Put(key interface{}, val interface{}) {
    ctm.mutex.Lock()
    ctm.tm.Put(key, val)
    ctm.mutex.Unlock()
}

func (ctm *ConcurrentTreeMap) Remove(key interface{}) {
    ctm.mutex.Lock()
    ctm.tm.Remove(key)
    ctm.mutex.Unlock()
}

func (ctm *ConcurrentTreeMap) Clear() {
    ctm.mutex.Lock()
    ctm.tm.Clear()
    ctm.mutex.Unlock()
}


func (ctm *ConcurrentTreeMap) Max() (interface{}, interface{}) {
    ctm.mutex.RLock()
    defer ctm.mutex.RUnlock()
    return ctm.tm.Max()
}

func (ctm *ConcurrentTreeMap) Find(f func(key interface{}, value interface{}) bool) (interface{}, interface{}) {
    ctm.mutex.RLock()
    defer ctm.mutex.RUnlock()
    return ctm.tm.Find(f)
}
