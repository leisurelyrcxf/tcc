package tcc

import (
    "fmt"
    "github.com/golang/glog"
    "testing"
)

func Exec() (err error) {
    return error(err)
}

func TestTxnError(t *testing.T) {
    err := Exec()
    fmt.Println(err)
    if err != nil {
        t.Errorf("not nil")
    }
}

func GetAllPossibleSerializableResult(db *DB, txns []*Txn, initDBFunc func(db *DB)) ([]map[string]float64, error) {
    initDBFunc(db)

    var allPossibleExecuteResults []map[string]float64
    var ten TxEngineNaive
    ch := Permutate(txns)
    for oneOrderTxns := range ch {
        if oneOrderTxns == nil {
            continue
        }

        initDBFunc(db)
        if err := ten.ExecuteTxns(db, oneOrderTxns); err != nil {
            newErr := fmt.Errorf("execute failed for oneOrderTxns, detail: '%s'", err.Error())
            fmt.Println(newErr.Error())
            return nil, newErr
        }

        oneResult := db.Snapshot()
        fmt.Printf("One possible serialize order: %s. Result: %s\n", SerializeTxns(oneOrderTxns), SerializeMap(oneResult))
        allPossibleExecuteResults = append(allPossibleExecuteResults, oneResult)
    }

    return allPossibleExecuteResults, nil
}

func TxTest(t *testing.T, db *DB, txns []*Txn, initDBFunc func(*DB),
    txnEngineConstructor func() TxEngine, round int) {
    initDBFunc(db)

    allPossibleExecuteResults, err := GetAllPossibleSerializableResult(db, txns, initDBFunc)
    if err != nil {
        t.Errorf(err.Error())
        return
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
            txn.Reset()
        }

        glog.Infof("\nRound: %d\n", i)
        if err := executeTxns(db, txns, txnEngineConstructor); err != nil {
            t.Errorf(err.Error())
            return
        }
        if !checkResult(db) {
            t.Errorf("result %s not conflict serializable", SerializeMap(db.Snapshot()))
            return
        }
    }
}

func executeTxns(db* DB, txns []*Txn, txnEngineConstructor func() TxEngine) error {
    return txnEngineConstructor().ExecuteTxns(db, txns)
}