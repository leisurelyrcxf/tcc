package tcc

import (
    "fmt"
    "github.com/golang/glog"
    "tcc/assert"
    "tcc/data_struct"
)

func NewDBValue(val float64, writtenTxn *Txn) DBValue {
    return DBValue{
        Value:   val,
        WrittenTxn: writtenTxn,
    }
}

type DBVersionedValue struct {
    DBValue
    Version int64
}

func NewDBVersionedValue(val float64, writtenTxn *Txn, version int64) DBVersionedValue {
    return DBVersionedValue{
        DBValue: DBValue{
            Value:      val,
            WrittenTxn: writtenTxn,
        },
        Version: version,
    }
}

func NewDBVersionedValueWithDBValue(dbValue DBValue, version int64) DBVersionedValue {
    return DBVersionedValue{
        DBValue: dbValue,
        Version: version,
    }
}

type DBVersionedValues struct {
    *data_struct.ConcurrentTreeMap
}

func NewDBVersionedValues() DBVersionedValues {
    return DBVersionedValues{
        ConcurrentTreeMap: data_struct.NewConcurrentTreeMap(func(a, b interface{}) int {
            return -int(a.(int64) - b.(int64))
        }),
    }
}

func (vvs DBVersionedValues) Get(version int64) (DBValue, error) {
    val, ok := vvs.ConcurrentTreeMap.Get(version)
    if ok {
        return val.(DBValue), nil
    }
    return DBValue{}, VersionNotExist
}

func (vvs DBVersionedValues) Put(version int64, val float64, writtenTxn *Txn) {
    vvs.ConcurrentTreeMap.Put(version, NewDBValue(val, writtenTxn))
}

func (vvs DBVersionedValues) FindMaxBelow(upperVersion int64) (DBVersionedValue, error) {
    version, dbVal := vvs.Find(func(key interface{}, value interface{}) bool {
        return key.(int64) <= upperVersion
    })
    if version == nil {
        return DBVersionedValue{}, VersionNotExist
    }
    return NewDBVersionedValueWithDBValue(dbVal.(DBValue), version.(int64)), nil
}

type DB struct {
    values            data_struct.ConcurrentMap
    ts                *TimeServer
    lm                *LockManager
    mvccEnabled       bool
}

var KeyNotExist = fmt.Errorf("key not exist")
var VersionNotExist = fmt.Errorf("version not exist")

func NewDB() *DB {
    db := &DB{
        values: data_struct.NewConcurrentMap(1024),
        ts: NewTimeServer(),
        lm: NewLockManager(),
    }
    return db
}

func NewDBWithMVCCEnabled() *DB {
    db := NewDB()
    db.mvccEnabled = true
    return db
}

type DBValue struct {
    Value float64
    WrittenTxn *Txn
}

func (db *DB) GetDBVersionedValues(key string) (DBVersionedValues, error) {
    val, ok := db.values.Get(key)
    if !ok {
        return DBVersionedValues{}, KeyNotExist
    }
    return val.(DBVersionedValues), nil
}

func (db *DB) GetDBValueMaxVersionBelow(key string, upperVersion int64) (DBVersionedValue, error) {
    vvs, err := db.GetDBVersionedValues(key)
    if err != nil {
        return DBVersionedValue{}, err
    }
    return vvs.FindMaxBelow(upperVersion)
}

func (db *DB) GetDBValue(key string) (DBVersionedValue, error) {
    if val, ok := db.values.Get(key); ok {
        return val.(DBVersionedValue), nil
    }
    return DBVersionedValue{}, KeyNotExist
}

// Non thread-safe
func (db *DB) Get(key string) (float64, error) {
    if val, ok := db.values.Get(key); ok {
        vv := val.(DBVersionedValue)
        return vv.Value, nil
    }
    return 0.0, KeyNotExist
}

func (db *DB) mustGetTreeMapValue(key string, holdsWriteLock bool) DBVersionedValues {
    val, ok := db.values.Get(key)
    if ok {
        return val.(DBVersionedValues)
    }

    if !holdsWriteLock {
        db.lm.UpgradeLock(key)
        defer db.lm.DegradeLock(key)
    }

    val, ok = db.values.Get(key)
    if ok {
        return val.(DBVersionedValues)
    }
    vvs := NewDBVersionedValues()
    db.values.Set(key, vvs)
    return vvs
}

func (db *DB) SetMVCC(key string, val float64, writtenTxn *Txn, holdsWriteLock bool) {
    vvs := db.mustGetTreeMapValue(key, holdsWriteLock)
    vvs.Put(writtenTxn.GetTimestamp(), val, writtenTxn)
}

func (db *DB) MustRemoveVersion(key string, version int64) {
    vvs, err := db.GetDBVersionedValues(key)
    assert.MustNoError(err)
    _, err = vvs.Get(version)
    assert.MustNoError(err)
    vvs.Remove(version)
}

func (db *DB) SetSafe(key string, val float64, writtenTxn *Txn) bool {
    assert.Must(!db.mvccEnabled)
    version := writtenTxn.GetTimestamp()
    setted, prevVal := db.values.SetIf(key, NewDBVersionedValue(val, writtenTxn, version), func(prev interface{}, exist bool) bool {
        return !exist || version >= prev.(DBVersionedValue).Version
    })
    if !setted {
        glog.V(5).Infof("ignored txn(%s) committed value %f for key '%s'," +
            " version_num_of_txn(%d) < committed_version(%d)",
            writtenTxn.String(), val, key, version, prevVal.(DBVersionedValue).Version)
    }
    return setted
}

func (db *DB) SetUnsafe(key string, val float64, version int64, writtenTxn *Txn) {
    if !db.mvccEnabled {
        db.values.Set(key, NewDBVersionedValue(val, writtenTxn, version))
        return
    }
    db.SetMVCC(key, val, emptyTx, false)
}

func (db *DB) Snapshot() map[string]float64 {
    m := make(map[string]float64)
    db.values.ForEachStrict(func(k string, vv interface{}) {
        m[k] = vv.(DBVersionedValue).Value
    })
    return m
}