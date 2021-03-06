package tcc

import (
    "container/list"
    "fmt"
    "reflect"
    "strconv"
    "strings"
    "tcc/assert"
)

func Permutate(txns []*Txn) chan[]*Txn {
    // Must be empty here
    ch := make(chan[]*Txn)
    go func() {
        permutate(txns, 0, ch)
        close(ch)
    }()
    return ch
}

func permutate(txns []*Txn, idx int, ch chan[]*Txn) {
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

func swap(txns []*Txn, i, j int) {
    txns[i], txns[j] = txns[j], txns[i]
}

func SerializeTxns(txns []*Txn) string {
    bytes := make([]byte, 0, 100)
    for i, txn := range txns {
        if i >= 1 {
            bytes = append(bytes, fmt.Sprintf(", %d", txn.ID)...)
        } else {
            bytes = append(bytes, fmt.Sprintf("%d", txn.ID)...)
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

func Min(a int64, b int64) int64 {
    if a > b {
        return b
    }
    return a
}

func MaxInt(a int, b int) int {
    if a > b {
        return a
    }
    return b
}

func Max(a int64, b int64) int64 {
    if a > b {
        return a
    }
    return b
}

func checkEqual(db *DB, m map[string]float64) bool {
    return areEqualMaps(db.Snapshot(), m)
}

func areEqualMaps(m1 map[string]float64, m2 map[string]float64) bool {
    if len(m1) != len(m2) {
        return false
    }
    for k, v := range m2 {
        if dbVal, ok := m1[k]; !ok {
            return false
        } else if dbVal != v {
            return false
        }
    }
    return true
}

func Array2String(objs interface{}, strMapper func(interface{})string) string {
    assert.Must(reflect.TypeOf(objs).Kind() == reflect.Array || reflect.TypeOf(objs).Kind() == reflect.Slice)
    sa := reflect.ValueOf(objs)
    strs := make([]string, sa.Len())
    for i := 0; i < sa.Len(); i++ {
        strs[i] = strMapper(sa.Index(i).Interface())
    }
    return strings.Join(strs, ", ")
}

func txnListStr(l *list.List) string {
    strs := make([]string, l.Len())
    i := 0
    for cur := l.Front(); cur != nil; cur = cur.Next() {
        strs[i] = fmt.Sprintf("%d", cur.Value.(*Txn).GetTimestamp())
        i++
    }
    return strings.Join(strs, "->")
}