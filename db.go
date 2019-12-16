package ts_promote

import "fmt"

type DB struct {
    values map[string]float64
    ts *TimeServer
}

var KeyNotExist = fmt.Errorf("key not exist")

func NewDB() *DB {
    return &DB{
        values: make(map[string]float64),
        ts: NewTimeServer(),
    }
}

// Non thread-safe
func (db *DB) get(key string) (float64, error) {
    if val, ok := db.values[key]; ok {
        return val, nil
    }
    return 0.0, KeyNotExist
}

func (db *DB) set(key string, val float64) {
    db.values[key] = val
}