package ts_promote

import (
    "fmt"
    "testing"
)

func GetAllPossibleSerializableResult(db *DB, txns []*Tx, initDBFunc func(db *DB)) ([]map[string]float64, error) {
    initDBFunc(db)

    for _, txn := range txns {
        txn.Timestamp = db.ts.FetchTimestamp()
    }

    var allPossibleExecuteResults []map[string]float64
    var ten TxEngineNaive
    ch := Permutate(txns)
    for oneOrderTxns := range ch {
        if oneOrderTxns == nil {
            continue
        }

        initDBFunc(db)
        if err := ten.ExecuteTxns(db, oneOrderTxns); err != nil {
            err = fmt.Errorf("execute failed for oneOrderTxns, detail: '%s'", err.Error())
            fmt.Println(err.Error())
            return nil, err
        }

        oneResult := make(map[string]float64)
        oneResult["a"], _ = db.get("a")
        oneResult["b"], _ = db.get("b")
        fmt.Printf("One possible serialize order: %s. Result: %s\n", SerializeTxns(oneOrderTxns), SerializeMap(oneResult))
        allPossibleExecuteResults = append(allPossibleExecuteResults, oneResult)
    }

    return allPossibleExecuteResults, nil
}

func TxTest(t *testing.T, txns []*Tx, initDBFunc func(*DB),
    txnEngineConstructor func() TxEngine, round int) {
    db := NewDB()
    initDBFunc(db)

    allPossibleExecuteResults, err := GetAllPossibleSerializableResult(db, txns, initDB)
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

    for i := 0; i < round; i++ {
        initDBFunc(db)
        for _, txn := range txns {
            txn.Timestamp = 0
        }
        if err := txEngineTOExecuteTxnsOneRound(db, txns, txnEngineConstructor); err != nil {
            t.Errorf(err.Error())
            return
        }
        if !checkResult(db) {
            t.Errorf("result %s not conflict serializable", SerializeMap(db.Snapshot()))
            return
        }
    }
}

func txEngineTOExecuteTxnsOneRound(db* DB, txns []*Tx, txnEngineConstructor func() TxEngine) error {
    return txnEngineConstructor().ExecuteTxns(db, txns)
}