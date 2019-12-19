package tcc

import (
    "fmt"
    "github.com/golang/glog"
    "tcc/data_struct"
)

type DB struct {
    values            data_struct.ConcurrentMap
    ts                *TimeServer
    lm                *LockManager
    committedVersions *data_struct.ConcurrentTreeMap
}

var KeyNotExist = fmt.Errorf("key not exist")

func NewDB() *DB {
    db := &DB{
        values: data_struct.NewConcurrentMap(1024),
        ts: NewTimeServer(),
        lm: NewLockManager(),
        committedVersions: data_struct.NewConcurrentTreeMap(func(a, b interface{}) int {
            return -(int(a.(int64) - b.(int64)))
        }),
    }
    return db
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

func (db *DB) GetDBValue(key string) (DBValue, error) {
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

func (db *DB) SetSafe(key string, val float64, writtenTxn *Txn) bool {
    version := writtenTxn.GetTimestamp()
    setted, prevVal := db.values.SetIf(key, NewDBValue(val, version, writtenTxn), func(prev interface{}, exist bool) bool {
        return !exist || version >= prev.(DBValue).Version
    })
    if !setted {
        glog.V(5).Infof("ignored txn(%s) committed value %f for key '%s'," +
            " version_num_of_txn(%d) < committed_version(%d)",
            writtenTxn.String(), val, key, version, prevVal.(DBValue).Version)
    }
    return setted
}

func (db *DB) SetUnsafe(key string, val float64, version int64, writtenTxn *Txn) {
    db.values.Set(key, NewDBValue(val, version, writtenTxn))
}

func (db *DB) AddVersion(version int64) {
    db.committedVersions.Put(version, nil)
}

func (db *DB) Snapshot() map[string]float64 {
    m := make(map[string]float64)
    db.values.ForEachStrict(func(k string, vv interface{}) {
        m[k] = vv.(DBValue).Value
    })
    return m
}

func (db *DB) FindMaxVersion(filter func(int64) bool) int64 {
    found, _ := db.committedVersions.Find(func(key interface{}, value interface{}) bool {
        return filter(key.(int64))
    })
    if found != nil {
        return found.(int64)
    }
    return 0
}