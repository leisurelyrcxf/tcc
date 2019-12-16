package ts_promote

import (
    "fmt"
    "ts_promote/data_struct"
)

type DB struct {
    values data_struct.ConcurrentMap
    ts *TimeServer
    lm *LockManager
}

var KeyNotExist = fmt.Errorf("key not exist")

func NewDB() *DB {
    return &DB{
        values: data_struct.NewConcurrentMap(1024),
        ts: NewTimeServer(),
        lm: NewLockManager(),
    }
}

// Non thread-safe
func (db *DB) get(key string) (float64, error) {
    if val, ok := db.values.Get(key); ok {
        return val, nil
    }
    return 0.0, KeyNotExist
}

func (db *DB) set(key string, val float64) {
    db.values.Set(key, val)
}

func (db *DB) Snapshot() map[string]float64 {
    m := make(map[string]float64)
    db.values.ForEach(func(k string, val float64) {
        m[k] = val
    })
    return m
}