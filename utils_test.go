package ts_promote

import (
    "fmt"
    "testing"
)

func TestPermutate(t *testing.T) {
    db := NewDB()

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

    for _, txn := range txns {
        txn.Timestamp = db.ts.FetchTimestamp()
    }

    ch := Permutate(txns)
    for txns := range ch {
        if txns == nil {
            continue
        }
        fmt.Println(SerializeTxns(txns))
    }
}

func TestSerializeMap(t *testing.T) {
    m := map[string]float64 {
        "a": 1.1,
        "b": 2.2222,
    }
    fmt.Println(SerializeMap(m))

    delete(m, "a")
    fmt.Println(SerializeMap(m))

    delete(m, "b")
    fmt.Println(SerializeMap(m))

}
