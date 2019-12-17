package tcc

import "testing"

func TestNewTxEngineC2PL(t *testing.T) {
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
    ), NewTx(
        []Op {{
            key: "b",
            typ: IncrMultiply,
            operatorNum: 20,
        }, {
            key: "a",
            typ: IncrAdd,
            operatorNum: 10,
        }},
    ), NewTx(
        []Op {{
            key:         "a",
            typ:         WriteDirect,
            operatorNum: 100,
        }},
    ),
    }

    db := NewDB()
    TxTest(t, db, txns, func (db *DB) {
        db.SetUnsafe("a", 0, 0, nil)
        db.SetUnsafe("b", 1, 0, nil)
        db.ts.c.Set(0)
        db.versions.Clear()
    }, func() TxEngine {
        return NewTxEngineC2PL(4)
    }, 10000)
}


