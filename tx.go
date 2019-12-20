package tcc

import (
    "container/list"
    "fmt"
    "github.com/golang/glog"
    "math"
    "sync/atomic"
    "tcc/assert"
    "tcc/data_struct"
    "tcc/expr"
    "tcc/sync2"
    "time"
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

type Context map[string]float64

type Txn struct {
    // Readonly fields
    ID   int64
    Ops  []Op
    Keys []string

    // Changeable fields.
    commitData   map[string]float64
    readVersions map[string]int64
    timestamp    sync2.AtomicInt64

    status       sync2.AtomicInt32

    firstOpMet   bool

    next         *Txn
    prev         *Txn

    ctx          Context

    waiters    *data_struct.ConcurrentMap
    waitingFor atomic.Value
    done       chan struct{}
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
    return txn
}

func (tx *Txn) Clone() *Txn {
    var clonedKeys []string
    if tx.Keys != nil {
        clonedKeys = make([]string, len(tx.Keys))
        copy(clonedKeys, tx.Keys)
    }
    newTxn := &Txn{
        ID:   tx.ID,
        Ops:  tx.Ops,
        Keys: clonedKeys,

        commitData:   make(map[string]float64),
        readVersions: make(map[string]int64),
        timestamp:    sync2.NewAtomicInt64(0),

        status:       sync2.NewAtomicInt32(int32(TxStatusInitialized)),

        firstOpMet:   false,

        prev:         tx,
        next:         nil,

        ctx:          make(map[string]float64),
    }
    tx.next = newTxn
    tx.GC()
    return newTxn
}

func (tx *Txn) GC() {
    tx.commitData = nil
    tx.readVersions = nil
    tx.ctx = nil
    // Dangerous don't do this
    //tx.done = nil
    //tx.wakeup = nil
}

func (tx *Txn) Clear() {
    tx.ClearCommitData()
    tx.ClearReadVersions()
    tx.ClearContext()
}

func (tx *Txn) ClearCommitData() {
    tx.commitData = make(map[string]float64)
}

func (tx *Txn) ClearReadVersions() {
    tx.readVersions = make(map[string]int64)
}

func (tx *Txn) ClearContext() {
    tx.ctx = make(map[string]float64)
}

// Only used in tests.
func (tx *Txn) ResetForTestOnly() {
    tx.timestamp.Set(0)

    tx.SetStatusLocked(TxStatusInitialized)

    tx.firstOpMet = false

    tx.next = nil
    tx.prev = nil

    tx.Clear()

    glog.V(10).Infof("Reset txn(%s)", tx.String())
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
    if tx.Keys != nil {
        return tx.Keys
    }
    var keys []string
    for _, op := range tx.Ops {
        if op.key == "" {
            continue
        }
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

func (tx *Txn) Start(ts *TimeServer, tid int, waitInterval time.Duration) {
    assert.Must(tx.timestamp.Get() == 0)
    assert.Must(len(tx.commitData) == 0)
    assert.Must(len(tx.readVersions) == 0)
    assert.Must(tx.GetStatus() == TxStatusInitialized)
    assert.Must(!tx.firstOpMet)

    time.Sleep(time.Duration(tid) * waitInterval)
    tx.timestamp.Set(ts.FetchTimestamp())

    tx.SetStatusLocked(TxStatusPending)

    c := data_struct.NewConcurrentMap(256)
    tx.done     = make(chan struct{})
    tx.waiters  = &c

    glog.V(10).Infof("Start txn(%s), status: '%s'", tx.String(), tx.GetStatus().String())
}

func (tx *Txn) Done(status TxStatus) {
    assert.Must(status.Done())
    tx.SetStatusLocked(status)

    close(tx.done)

    glog.V(10).Infof("Done txn(%s), status: '%s'", tx.String(), status.String())
}

func (tx *Txn) GetTimestamp() int64 {
    if tx == nil {
        return math.MaxInt64
    }
    return tx.timestamp.Get()
}

func (tx *Txn) MustGetWaitersOfKey(key string, lm *LockManager) *list.List {
    var waitersOfKey *list.List
    lObj, ok := tx.waiters.Get(key)
    if ok {
        return lObj.(*list.List)
    }

    lm.UpgradeLock(key)
    defer lm.DegradeLock(key)

    lObj, ok = tx.waiters.Get(key)
    if ok {
        return lObj.(*list.List)
    }

    waitersOfKey = list.New()
    // tx is always the head.
    waitersOfKey.PushBack(tx)

    tx.waiters.Set(key, waitersOfKey)

    return waitersOfKey
}

func (tx *Txn) AddWaiter(key string, waiter *Txn, lm *LockManager) {
    waitersOfKey := tx.MustGetWaitersOfKey(key, lm)
    after, before := tx.insertWaiterForKey(waitersOfKey, waiter)
    //assert.Must(waiter.GetTimestamp() > before.GetTimestamp() && waiter.GetTimestamp() < after.GetTimestamp())
    waiter.SetWaitingFor(before)
    if after != nil {
      after.SetWaitingFor(waiter)
    }
}

func (tx *Txn) insertWaiterForKey(waiters *list.List, waiter *Txn) (after *Txn, before *Txn) {
    var cur, prev *list.Element
    for cur, prev = waiters.Front(), nil; cur != nil; cur, prev = cur.Next(), cur {
        if cur.Value.(*Txn).GetTimestamp() > waiter.GetTimestamp() {
            waiters.InsertBefore(waiter, cur)
            return cur.Value.(*Txn), prev.Value.(*Txn)
        }
    }
    waiters.InsertAfter(waiter, prev)
    return nil, prev.Value.(*Txn)
}

func (tx *Txn) SetWaitingFor(waitingFor *Txn) {
    tx.waitingFor.Store(waitingFor)
}

func (tx *Txn) Wait() {
    waiterDesc := tx.String()

    var waited *Txn
    for {
        waitingFor := tx.waitingFor.Load().(*Txn)
        if waitingFor == waited {
            return
        }

        //assert.Must(waited == nil || waited.GetTimestamp() < waitingFor.GetTimestamp())

        waitingForDesc := waitingFor.String()
        glog.V(5).Infof("txn(%s) wait for txn(%s) to finish", waiterDesc, waitingForDesc)
        <-waitingFor.done
        waited = waitingFor
        glog.V(5).Infof("txn(%s) waited txn(%s) successfully", waiterDesc, waitingForDesc)
    }
}

func (tx *Txn) WaitFor(waitFor *Txn) {
    assert.Must(tx.GetTimestamp() > waitFor.GetTimestamp())
    waiterDesc := tx.String()
    waitingForDesc := waitFor.String()
    glog.V(5).Infof("txn(%s) wait for txn(%s) to finish", waiterDesc, waitingForDesc)
    <-waitFor.done
    glog.V(5).Infof("txn(%s) waited txn(%s) successfully", waiterDesc, waitingForDesc)
}

func (tx *Txn) GetCommitData() map[string]float64 {
    return tx.commitData
}

func (tx *Txn) GetReadVersions() map[string]int64 {
    return tx.readVersions
}

func (tx *Txn) CheckFirstOp(ts *TimeServer) {
    if !enableFirstOpTimestampPromote {
        return
    }
    if !tx.firstOpMet {
        tx.firstOpMet = true
        tx.timestamp.Set(ts.FetchTimestamp())
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