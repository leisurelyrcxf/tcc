package ts_promote

import (
    "fmt"
    "github.com/golang/glog"
    "sync"
    "ts_promote/assert"
    "ts_promote/sync2"
)

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

type TxStatus int

const (
    TxStatusInitialized TxStatus = iota
    TxStatusPending
    TxStatusFailed
    TxStatusFailedRetryable
    TxStatusSucceeded
)

func (s TxStatus) String() string {
    switch s {
    case TxStatusInitialized:
        return "TxStatusInitialized"
    case TxStatusPending:
        return "TxStatusPending"
    case TxStatusFailed:
        return "TxStatusFailed"
    case TxStatusFailedRetryable:
        return "TxStatusFailedRetryable"
    case TxStatusSucceeded:
        return "TxStatusSucceeded"
    default:
        panic("unreachable code")
    }
}

func (s TxStatus) Done() bool {
    return s == TxStatusSucceeded || s == TxStatusFailed || s == TxStatusFailedRetryable
}

func (s TxStatus) HasError() bool {
    return s == TxStatusFailed || s == TxStatusFailedRetryable
}

var Counter = sync2.NewAtomicInt64(0)

type Txn struct {
    // Readonly fields
    TxId       int64
    Ops        []Op

    // Changeable fields.
    commitData map[string]float64
    Timestamp  int64

    round    sync2.AtomicInt32
    status sync2.AtomicInt32

    mutex    sync.Mutex
    doneCond sync.Cond
}

var emptyTx = &Txn{}

func NewTx(ops []Op) *Txn {
    txn := &Txn{
        TxId:       Counter.Add(1),
        Ops:        ops,
        commitData: make(map[string]float64),

        round: sync2.NewAtomicInt32(0),
        status: sync2.NewAtomicInt32(int32(TxStatusInitialized)),
    }
    txn.doneCond = sync.Cond{
        L: &txn.mutex,
    }
    return txn
}

func (tx *Txn) AddCommitData(key string, val float64) {
    tx.commitData[key] = val
}

func (tx *Txn) CollectKeys() []string {
    var keys []string
    for _, op := range tx.Ops {
        keys = append(keys, op.key)
    }
    return keys
}

func (tx *Txn) Done(status TxStatus) {
    tx.mutex.Lock()

    assert.Must(status.Done())
    tx.status.Set(int32(status))
    tx.round.Add(1)

    tx.mutex.Unlock()

    tx.doneCond.Broadcast()
    glog.V(10).Infof("Done txn(%s), status: '%s'", tx.String(), status.String())
}

func (tx *Txn) String() string {
    return fmt.Sprintf("%d, %d", tx.TxId, tx.Timestamp)
}

func (tx *Txn) GetStatus() TxStatus {
    return TxStatus(tx.status.Get())
}

func (tx *Txn) GetRound() int32 {
    return tx.round.Get()
}

func (tx *Txn) WaitTillDone(round int32, waiter *Txn) {
    if tx.GetRound() == round {
        tx.mutex.Lock()

        for tx.GetRound() == round {
            glog.Infof("txn(%s) wait for txn(%s) to finish", waiter.String(), tx.String())
            tx.doneCond.Wait()
            glog.Infof("txn(%s) waited txn(%s) successfully", waiter.String(), tx.String())
        }

        tx.mutex.Unlock()
    }
}

func (tx *Txn) ReInit() {
    assert.Must(len(tx.commitData) == 0)
    tx.status = sync2.NewAtomicInt32(int32(TxStatusInitialized))
    glog.Infof("ReInit txn(%s)", tx.String())
}

func (tx *Txn) GetCommitData() map[string]float64 {
    return tx.commitData
}

func (tx *Txn) ClearCommitData() {
    tx.commitData = make(map[string]float64)
}

func Later(a *Txn, b *Txn) *Txn {
    if a.Timestamp > b.Timestamp {
        return a
    }
    return b
}

type TxnError struct {
    Err       error
    Retryable bool
}

func NewTxnError(err error, retryable bool) *TxnError {
    assert.Must(err != nil)
    return &TxnError{
        Err:       err,
        Retryable: retryable,
    }
}

func (e *TxnError) Error() string {
    return e.Err.Error()
}

func (e *TxnError) IsRetryable() bool {
    return e.Retryable
}

type TxnSliceSortByTimestamp []*Txn

func (ts TxnSliceSortByTimestamp) Len() int {
    return len(ts)
}

func (ts TxnSliceSortByTimestamp) Less(i, j int) bool {
    return ts[i].Timestamp < ts[j].Timestamp
}

func (ts TxnSliceSortByTimestamp) Swap(i, j int) {
    ts[i], ts[j] = ts[j], ts[i]
}

type TxnSliceSortByTxID []*Txn

func (ts TxnSliceSortByTxID) Len() int {
    return len(ts)
}

func (ts TxnSliceSortByTxID) Less(i, j int) bool {
    return ts[i].TxId < ts[j].TxId
}

func (ts TxnSliceSortByTxID) Swap(i, j int) {
    ts[i], ts[j] = ts[j], ts[i]
}