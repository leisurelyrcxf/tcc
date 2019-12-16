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

type DBValue struct {
    Value float64
    Version int64
    WrittenTxn *Txn
}

func NewDBValue(val float64, version int64, writtenTxn *Txn) DBValue {
    return DBValue{
        Value:   val,
        Version: version,
        WrittenTxn: writtenTxn,
    }
}

func (db *DB) GetVersionedValue(key string) (DBValue, error) {
    if val, ok := db.values.Get(key); ok {
        return val.(DBValue), nil
    }
    return DBValue{}, KeyNotExist
}

// Non thread-safe
func (db *DB) Get(key string) (float64, error) {
    if val, ok := db.values.Get(key); ok {
        vv := val.(DBValue)
        return vv.Value, nil
    }
    return 0.0, KeyNotExist
}

func (db *DB) SetUnsafe(key string, val float64, version int64, writtenTxn *Txn) {
    db.values.Set(key, NewDBValue(val, version, writtenTxn))
}

func (db *DB) Snapshot() map[string]float64 {
    m := make(map[string]float64)
    db.values.ForEach(func(k string, vv interface{}) {
        m[k] = vv.(DBValue).Value
    })
    return m
}