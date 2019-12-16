package ts_promote

type OpType int

const (
    IncrMultiply OpType = iota
    IncrAdd
    IncrMinus
)

type Op struct {
    key string
    typ OpType
    operatorNum float64
}

type Tx struct {
    Timestamp int64
    Ops []Op
}

func NewTx(ops []Op) *Tx {
    return &Tx{
        Ops: ops,
    }
}