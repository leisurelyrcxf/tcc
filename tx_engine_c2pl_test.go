package ts_promote

import "testing"

func initDB(db *DB) {
    db.set("a", 0)
    db.set("b", 1)
    db.ts.c.Set(0)
}

func TestNewTxEngineC2PL(t *testing.T) {
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

    TxTest(t, txns, initDB, func() TxEngine {
        return NewTxEngineC2PL(4)
    }, 10000)
}


