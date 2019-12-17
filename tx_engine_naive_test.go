package tcc

import (
    "fmt"
    "testing"
)

func TestTxEngineNaive_ExecuteTxns(t *testing.T) {
    db := NewDB()
    db.SetUnsafe("a", 1.0, 0, nil)
    db.SetUnsafe("b", 2.0, 0, nil)

    txns := []*Txn{NewTx(
        []Op {{
            key: "a",
            typ: IncrAdd,
            operatorNum: 1,
        }, {
            key: "b",
            typ: IncrAdd,
            operatorNum: 2,
        }},
    ), NewTx(
        []Op {{
            key: "a",
            typ: IncrAdd,
            operatorNum: 1,
        }, {
            key: "b",
            typ: IncrAdd,
            operatorNum: 2,
        }},
    ), NewTx(
        []Op {{
            key: "a",
            typ: IncrAdd,
            operatorNum: 1,
        }, {
            key: "b",
            typ: IncrAdd,
            operatorNum: 2,
        }},
    )}

    var te TxEngineNaive
    err := te.ExecuteTxns(db, txns)
    if err != nil {
        t.Errorf(err.Error())
        return
    }
    if val, _ := db.Get("a"); val != 4 {
        t.Errorf("value for a failed")
        return
    }

    if val, _ := db.Get("b"); val != 8 {
        t.Errorf("value for a failed")
        return
    }
    for _, txn := range txns {
        fmt.Println(txn.GetTimestamp())
    }
}
