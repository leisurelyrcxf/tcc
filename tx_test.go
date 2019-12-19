package tcc

import (
    "fmt"
    "github.com/golang/glog"
    "testing"
    "time"
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
    ten := NewTxEngineNaive(db)
    ch := Permutate(txns)
    for oneOrderTxns := range ch {
        if oneOrderTxns == nil {
            continue
        }
        for _, txn := range oneOrderTxns {
            txn.ResetForTestOnly()
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

    start := time.Now()
    var totalTime time.Duration
    for i := 0; i < round; i++ {
        initDBFunc(db)
        for _, txn := range txns {
            txn.ResetForTestOnly()
        }

        glog.V(10).Infof("\nRound: %d\n", i)
        if err := executeTxns(db, txns, txnEngineConstructor); err != nil {
            t.Errorf(err.Error())
            return
        }

        totalTime += time.Since(start)

        if !checkResult(db) {
            t.Errorf("result %s not conflict serializable", SerializeMap(db.Snapshot()))
            return
        }
        if i % 100 == 0 {
            fmt.Printf("%d rounds finished\n", i)
        }

        start = time.Now()
    }
    fmt.Printf("\nCost %f seconds for %d rounds\n", float64(totalTime)/float64(time.Second), round)
}

func executeTxns(db* DB, txns []*Txn, txnEngineConstructor func() TxEngine) error {
    return txnEngineConstructor().ExecuteTxns(db, txns)
}