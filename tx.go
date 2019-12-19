package tcc

import (
    "fmt"
    "github.com/golang/glog"
    "sync"
    "tcc/assert"
    "tcc/expr"
    "tcc/sync2"
)

type OpType int

const (
    IncrMultiply OpType = iota
    IncrAdd
    IncrMinus
    WriteDirect
    Procedure
)

// Soiyez prudent
const enableFirstOpTimestampPromote = true

func (ot OpType) IsIncr() bool {
    return int(ot) < int(WriteDirect)
}

type Op struct {
    typ OpType
    key string
    operatorNum float64
    expr expr.Expr
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
    return s.Succeeded() || s.HasError()
}

func (s TxStatus) HasError() bool {
    return s == TxStatusFailed || s == TxStatusFailedRetryable
}

var Counter = sync2.NewAtomicInt64(0)

type Txn struct {
    // Readonly fields
    ID  int64
    Ops []Op

    // Changeable fields.
    commitData   map[string]float64
    readVersions map[string]int64
    timestamp    sync2.AtomicInt64

    status       sync2.AtomicInt32

    firstOpMet   bool

    mutex        sync.Mutex
    cond         sync.Cond

    next         *Txn
    prev         *Txn

    ctx          expr.Context
}

const TxIDNaN = -1

var TxNaN = &Txn{
    ID:           TxIDNaN,
    Ops:          make([]Op, 0, 0),

    commitData:   make(map[string]float64),
    readVersions: make(map[string]int64),
    timestamp:    sync2.NewAtomicInt64(0),

    status:       sync2.NewAtomicInt32(int32(TxStatusFailed)),

    firstOpMet:   true,

    ctx:          make(map[string]float64),
}

func NewTx(ops []Op) *Txn {
    txn := &Txn{
        ID:  Counter.Add(1),
        Ops: ops,

        commitData:   make(map[string]float64),
        readVersions: make(map[string]int64),
        timestamp:    sync2.NewAtomicInt64(0),

        status:       sync2.NewAtomicInt32(int32(TxStatusInitialized)),

        firstOpMet:   false,
        ctx:          make(map[string]float64),
    }
    txn.cond = sync.Cond{
        L: &txn.mutex,
    }
    return txn
}

func (tx *Txn) Clone() *Txn {
    newTxn := &Txn{
        ID:  tx.ID,
        Ops: tx.Ops,

        commitData:   make(map[string]float64),
        readVersions: make(map[string]int64),
        timestamp:    sync2.NewAtomicInt64(0),

        status:       sync2.NewAtomicInt32(int32(TxStatusInitialized)),

        firstOpMet:   false,
        ctx:          make(map[string]float64),
    }
    newTxn.cond = sync.Cond{
        L: &newTxn.mutex,
    }
    tx.next = newTxn
    newTxn.prev = tx
    return newTxn
}

func (tx *Txn) AddCommitData(key string, val float64) {
    tx.commitData[key] = val
}

func (tx *Txn) AddReadVersion(key string, val int64) {
    _, containsKey := tx.readVersions[key]
    assert.Must(!containsKey)
    tx.readVersions[key] = val
}

func (tx *Txn) CollectKeys() []string {
    var keys []string
    for _, op := range tx.Ops {
        keys = append(keys, op.key)
    }
    return keys
}

func (tx *Txn) String() string {
    return fmt.Sprintf("%d, %d", tx.ID, tx.GetTimestamp())
}

func (tx *Txn) GetStatus() TxStatus {
    return TxStatus(tx.status.Get())
}

func (tx *Txn) SetStatusLocked(status TxStatus) {
    tx.status.Set(int32(status))
}

func (tx *Txn) Start(ts int64) {
    assert.Must(tx.timestamp.Get() == 0)
    assert.Must(len(tx.commitData) == 0)
    assert.Must(len(tx.readVersions) == 0)
    assert.Must(tx.GetStatus() == TxStatusInitialized)
    assert.Must(!tx.firstOpMet)

    tx.mutex.Lock()

    tx.timestamp.Set(ts)
    tx.SetStatusLocked(TxStatusPending)

    tx.mutex.Unlock()

    tx.cond.Broadcast()
    glog.V(10).Infof("Start txn(%s), status: '%s'", tx.String(), tx.GetStatus().String())
}

func (tx *Txn) Done(status TxStatus) {
    tx.mutex.Lock()

    assert.Must(status.Done())
    tx.SetStatusLocked(status)

    tx.mutex.Unlock()

    tx.cond.Broadcast()
    glog.V(10).Infof("Done txn(%s), status: '%s'", tx.String(), status.String())
}

func (tx *Txn) GetTimestamp() int64 {
    if tx == nil {
        return 0
    }
    return tx.timestamp.Get()
}

func (tx *Txn) WaitUntilDone(waiter *Txn) {
    assert.Must(tx != waiter)
    waiterDesc := waiter.String()

    tx.mutex.Lock()

    glog.V(5).Infof("txn(%s) wait for txn(%s) to finish", waiterDesc, tx.String())
    loopTimes := 0
    for !tx.GetStatus().Done()  {
        if loopTimes > 0 {
            glog.V(5).Infof("txn(%s) waited once txn(%s) successfully", waiterDesc, tx.String())
            glog.V(5).Infof("txn(%s) wait once for txn(%s) to finish", waiterDesc, tx.String())
        }
        tx.cond.Wait()
        loopTimes++
    }
    glog.V(5).Infof("txn(%s) waited txn(%s) successfully", waiterDesc, tx.String())

    tx.mutex.Unlock()
}

// Only used in tests.
func (tx *Txn) ResetForTestOnly() {
    tx.mutex.Lock()

    tx.ClearCommitData()
    tx.ClearReadVersions()
    tx.timestamp.Set(0)

    tx.SetStatusLocked(TxStatusInitialized)

    tx.firstOpMet = false

    tx.next = nil
    tx.prev = nil

    tx.ctx = make(map[string]float64)

    tx.mutex.Unlock()

    tx.cond.Broadcast()
    glog.V(10).Infof("Reset txn(%s)", tx.String())
}

func (tx *Txn) GetCommitData() map[string]float64 {
    return tx.commitData
}

func (tx *Txn) GetReadVersions() map[string]int64 {
    return tx.readVersions
}

func (tx *Txn) ClearCommitData() {
    tx.commitData = make(map[string]float64)
}

func (tx *Txn) ClearReadVersions() {
    tx.readVersions = make(map[string]int64)
}

func (tx *Txn) CheckFirstOp(ts *TimeServer) {
    if !enableFirstOpTimestampPromote {
        return
    }
    if !tx.firstOpMet {
        tx.firstOpMet = true
        tx.timestamp.Set(ts.FetchTimestamp())
        tx.cond.Broadcast()
    }
}

func (tx *Txn) Head() *Txn {
    for cur, prev := tx, tx.prev; ; cur, prev = prev, prev.prev {
        if prev == nil {
            return cur
        }
    }
}

func (tx *Txn) Tail() *Txn {
    for cur, next := tx, tx.next; ; cur, next = next, next.next {
        if next == nil {
            return cur
        }
    }
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
    return ts[i].ID < ts[j].ID
}

func (ts TxnSliceSortByTxID) Swap(i, j int) {
    ts[i], ts[j] = ts[j], ts[i]
}