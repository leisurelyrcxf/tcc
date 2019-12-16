package ts_promote

import (
    "fmt"
    "testing"
)

func TestTxEngineNaive_ExecuteTxns(t *testing.T) {
    db := NewDB()
    db.set("a", 1.0)
    db.set("b", 2.0)

    txns := []*Tx{NewTx(
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
    if val, _ := db.get("a"); val != 4 {
        t.Errorf("value for a failed")
        return
    }

    if val, _ := db.get("b"); val != 8 {
        t.Errorf("value for a failed")
        return
    }
    for _, txn := range txns {
        fmt.Println(txn.Timestamp)
    }
}
