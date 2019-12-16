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
    WriteDirect
)

func (ot OpType) IsIncr() bool {
    return int(ot) < int(WriteDirect)
}

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

func (s TxStatus) Succeeded() bool {
    return s == TxStatusSucceeded
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
    timestamp  sync2.AtomicInt64

    round    sync2.AtomicInt32
    status sync2.AtomicInt32

    ReadVersion int64

    mutex sync.Mutex
    cond  sync.Cond
}

var emptyTx = &Txn{}

func NewTx(ops []Op) *Txn {
    txn := &Txn{
        TxId:       Counter.Add(1),
        Ops:        ops,
        commitData: make(map[string]float64),

        round: sync2.NewAtomicInt32(0),
        status: sync2.NewAtomicInt32(int32(TxStatusInitialized)),

        ReadVersion: 0,
    }
    txn.cond = sync.Cond{
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

func (tx *Txn) String() string {
    return fmt.Sprintf("%d, %d", tx.TxId, tx.GetTimestamp())
}

func (tx *Txn) GetStatus() TxStatus {
    return TxStatus(tx.status.Get())
}

func (tx *Txn) SetStatusLocked(status TxStatus) {
    tx.status.Set(int32(status))
}

func (tx *Txn) Start(ts int64) {
    tx.mutex.Lock()

    tx.timestamp.Set(ts)
    tx.SetStatusLocked(TxStatusPending)
    tx.ReadVersion = 0

    tx.mutex.Unlock()

    tx.cond.Broadcast()
    glog.V(10).Infof("Start txn(%s), status: '%s', round: %d", tx.String(), tx.GetStatus().String(), tx.round.Get())
}

func (tx *Txn) Done(status TxStatus) {
    tx.mutex.Lock()

    assert.Must(status.Done())
    tx.SetStatusLocked(status)
    tx.IncrRound()

    tx.mutex.Unlock()

    tx.cond.Broadcast()
    glog.V(10).Infof("Done txn(%s), status: '%s', new round: %d", tx.String(), status.String(), tx.round.Get())
}

func (tx *Txn) GetTimestamp() int64 {
    return tx.timestamp.Get()
}

func (tx *Txn) GetRound() int32 {
    return tx.round.Get()
}

func (tx *Txn) IncrRound() {
    tx.round.Add(1)
}

func (tx *Txn) WaitUntilDoneOrRestarted(round int32, waiter *Txn) {
    tx.mutex.Lock()

    glog.V(5).Infof("txn(%s) wait for txn(%s)'s %dth round to finish", waiter.String(), tx.String(), round)
    loopTimes := 0
    for tx.GetRound() == round && !tx.GetStatus().Done() && waiter.GetTimestamp() > tx.GetTimestamp()  {
        if loopTimes > 0 {
            glog.V(5).Infof("txn(%s) wait for txn(%s)'s %dth round to finish", waiter.String(), tx.String(), round)
        }
        tx.cond.Wait()
        glog.V(5).Infof("txn(%s) waited once txn(%s)'s %dth round successfully", waiter.String(), tx.String(), round)
        loopTimes++
    }
    if loopTimes == 0 {
        glog.V(5).Infof("txn(%s) waited txn(%s)'s %dth round successfully", waiter.String(), tx.String(), round)
    }
    tx.mutex.Unlock()
}

func (tx *Txn) WaitUntilDone(waiter *Txn) {
    tx.mutex.Lock()

    glog.V(5).Infof("txn(%s) wait for txn(%s)'s to be done", waiter.String(), tx.String())
    loopTimes := 0
    for !tx.GetStatus().Done()  {
        if loopTimes > 0 {
            glog.V(5).Infof("txn(%s) wait for txn(%s) to be done", waiter.String(), tx.String())
        }
        tx.cond.Wait()
        glog.V(5).Infof("txn(%s) waited once txn(%s) done successfully", waiter.String(), tx.String())
        loopTimes++
    }
    if loopTimes == 0 {
        glog.V(5).Infof("txn(%s) waited txn(%s) done successfully", waiter.String(), tx.String())
    }
    tx.mutex.Unlock()
}

func (tx *Txn) Reset() {
    tx.mutex.Lock()

    tx.ClearCommitData()
    tx.timestamp.Set(0)

    tx.round.Set(0)
    tx.SetStatusLocked(TxStatusInitialized)

    tx.ReadVersion = 0

    tx.mutex.Unlock()

    tx.cond.Broadcast()
    glog.V(10).Infof("Reset txn(%s)", tx.String())
}

func (tx *Txn) GetCommitData() map[string]float64 {
    return tx.commitData
}

func (tx *Txn) ClearCommitData() {
    tx.commitData = make(map[string]float64)
}

func Later(a *Txn, b *Txn) *Txn {
    if a.GetTimestamp() > b.GetTimestamp() {
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
    return ts[i].GetTimestamp() < ts[j].GetTimestamp()
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