package ts_promote

import (
    "fmt"
    "strconv"
)

func Permutate(txns []*Tx) chan[]*Tx {
    // Must be empty here
    ch := make(chan[]*Tx)
    go func() {
        permutate(txns, 0, ch)
        close(ch)
    }()
    return ch
}

func permutate(txns []*Tx, idx int, ch chan[]*Tx) {
    l := len(txns)
    if idx == l - 1 {
        ch <-txns
        // Wait till txns is consumed
        ch <-nil
        return
    }
    for j := idx; j < l; j++ {
        swap(txns, idx, j)
        permutate(txns, idx+1, ch)
        swap(txns, idx, j)
    }
}

func swap(txns []*Tx, i, j int) {
    txns[i], txns[j] = txns[j], txns[i]
}

func SerializeTxns(txns []*Tx) string {
    bytes := make([]byte, 0, 100)
    for i, txn := range txns {
        if i >= 1 {
            bytes = append(bytes, fmt.Sprintf(", %d", txn.Timestamp)...)
        } else {
            bytes = append(bytes, fmt.Sprintf("%d", txn.Timestamp)...)
        }
    }
    return string(bytes)
}

func SerializeMap(m map[string]float64) string {
    bytes := make([]byte, 0, 100)
    bytes = append(bytes, '{')
    for k, v := range m {
        bytes = append(bytes, []byte(k)...)
        bytes = append(bytes, ':')
        bytes = strconv.AppendFloat(bytes, v,  'g', -1, 64)
        bytes = append(bytes, ',', ' ')
    }
    if len(m) > 0 {
        bytes = bytes[:len(bytes)-2]
    }
    bytes = append(bytes, '}')
    return string(bytes)
}