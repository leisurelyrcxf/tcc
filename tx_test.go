package tcc

import (
    "fmt"
    "github.com/golang/glog"
    "math/rand"
    "sync"
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

func GetAllPossibleSerializableResult(db *DB, txns []*Txn, initDBFunc func(db *DB), logAll bool) ([]map[string]float64, error) {
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
            return nil, newErr
        }

        oneResult := db.Snapshot()
        if logAll {
            glog.V(1).Infof("One possible serialize order: %s. Result: %s\n", SerializeTxns(oneOrderTxns), SerializeMap(oneResult))
        }
        allPossibleExecuteResults = append(allPossibleExecuteResults, oneResult)
    }

    return allPossibleExecuteResults, nil
}

func TxTest(t *testing.T, db *DB, txns []*Txn, initDBFunc func(*DB),
    txnEngineConstructor func() TxEngine, round int, logAll bool) {
    initDBFunc(db)

    allPossibleExecuteResults, err := GetAllPossibleSerializableResult(db, txns, initDBFunc, logAll)
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

func TestAtomicTxn_SetIfIncrease(t *testing.T) {
    var wg sync.WaitGroup
    at := NewAtomicWaitForTxn(NewWaitForTxn(NewTx(nil), "abc", nil))
    writerNum := 10
    N := int64(1000000)

    for i := 0; i < writerNum; i++ {
        wg.Add(1)

        go func(tid int) {
            defer wg.Done()

            for j := int64(0); j < N; j++ {
                wtx := NewWaitForTxn(NewTx(nil), "abc", nil)
                wtx.timestamp.Set(j)
                at.SetIfSameKeyAndIncr(wtx)
                time.Sleep(time.Duration(rand.Int63n(10)) * time.Nanosecond)
            }
            //fmt.Printf("thread %d finished\writerNum", tid)
        }(i)
    }

    readerNum := 10
    for i := 0; i < readerNum; i++ {
        wg.Add(1)

        go func(tid int) {
            defer wg.Done()

            var waitForTxn, lastWaitForTxn *WaitForTxn
            for {
                waitForTxn = at.Load()
                if lastWaitForTxn != nil && waitForTxn.GetTimestamp() < lastWaitForTxn.GetTimestamp() {
                    t.Errorf("test failed, old: %d, new: %d", lastWaitForTxn.GetTimestamp(), waitForTxn.GetTimestamp())
                    return
                }
                ts := waitForTxn.GetTimestamp()
                if ts >= N-1 {
                    break
                }
                lastWaitForTxn = waitForTxn
                time.Sleep(time.Duration(rand.Int63n(5)) * time.Nanosecond)
            }
        }(i)
    }

    wg.Wait()
}