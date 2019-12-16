package ts_promote

import (
    "testing"
)

func initDB(db *DB) {
    db.set("a", 0)
    db.set("b", 1)
    db.ts.c.Set(0)
}

func TestTxEngineTO_ExecuteTxns(t *testing.T) {
    db := NewDB()
    initDB(db)

    txns := []*Tx{NewTx(
        []Op {{
            key: "a",
            typ: IncrAdd,
            operatorNum: 1,
        }, {
            key: "b",
            typ: IncrMultiply,
            operatorNum: 2,
        }},
    ), NewTx(
        []Op {{
            key: "a",
            typ: IncrMultiply,
            operatorNum: 2,
        }, {
            key: "b",
            typ: IncrAdd,
            operatorNum: 1,
        }},
    ),
    NewTx(
       []Op {{
           key: "b",
           typ: IncrMultiply,
           operatorNum: 20,
      }, {
           key: "a",
           typ: IncrAdd,
           operatorNum: 10,
       }},
    ),
    }

    allPossibleExecuteResults, err := GetAllPossibleSerializableResult(db, txns, initDB, true)
    if err != nil {
        t.Errorf(err.Error())
        return
    }

    checkEqual := func(db *DB, m map[string]float64) bool {
        for k, v := range m {
            if dbVal, err := db.get(k); err != nil {
                return false
            } else {
                if dbVal != v {
                    return false
                }
            }
        }
        return true
    }

    checkResult := func (db* DB) bool {
        for _, oneResult := range allPossibleExecuteResults {
            if checkEqual(db, oneResult) {
                return true
            }
        }
        return false
    }

    for i := 0; i < 1000; i++ {
        initDB(db)
        for _, txn := range txns {
            txn.Timestamp = 0
        }
        if err := txEngineTOExecuteTxnsOneRound(db, txns); err != nil {
            t.Errorf(err.Error())
            return
        }
        if !checkResult(db) {
            t.Errorf("result %s not conflict serializable", SerializeMap(db.Snapshot()))
            return
        }
    }
}

func txEngineTOExecuteTxnsOneRound(db* DB, txns []*Tx) error {
    te := NewTxEngineC2PL(4)
    return te.ExecuteTxns(db, txns)
}
