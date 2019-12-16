package ts_promote

import (
    "fmt"
    "sort"
    "testing"
)

func TestNewTxEngineTO(t *testing.T) {
    db := NewDB()
    txns := []*Txn{NewTx(
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

    initDBFunc := func (db *DB) {
        db.SetUnsafe("a", 0, 0)
        db.SetUnsafe("b", 1, 0)
        db.ts.c.Set(0)
    }

    round := 100000
    for i := 0; i < round; i++ {
        fmt.Printf("\nRound: %d\n", i)
        err := executeOneRound(db, txns, initDBFunc)
        if err != nil {
            t.Errorf(err.Error())
            return
        }
    }
}

func executeOneRound(db *DB, txns []*Txn, initDBFunc func(*DB)) error {
    initDBFunc(db)
    for _, txn := range txns {
        txn.Timestamp = 0
        txn.ClearCommitData()
        txn.ReInit()
    }

    if err := NewTxEngineTO(4, db.lm).ExecuteTxns(db, txns); err != nil {
        return err
    }
    toResult := db.Snapshot()

    if err := generateDatum(db, txns, initDBFunc); err != nil {
        return err
    }
    datum := db.Snapshot()
    if !areEqualMaps(toResult, datum) {
        return fmt.Errorf("expect '%s', but met '%s'", SerializeMap(datum), SerializeMap(toResult))
    }
    return nil
}

func generateDatum(db *DB, txns []*Txn, initDBFunc func(*DB)) error {
    sort.Sort(TxnSliceSortByTimestamp(txns))
    defer sort.Sort(TxnSliceSortByTxID(txns))

    initDBFunc(db)
    var ten TxEngineNaive
    if err := ten.ExecuteTxns(db, txns); err != nil {
        return fmt.Errorf("execute failed for oneOrderTxns, detail: '%s'", err.Error())
    }
    return nil
}


