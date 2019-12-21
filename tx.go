package tcc

import (
    "container/list"
    "fmt"
    "github.com/golang/glog"
    "sync/atomic"
    "tcc/assert"
    "tcc/data_struct"
    "tcc/expr"
    "tcc/sync2"
    "time"
    "unsafe"
)

const glogLevelTxnWaitingList = glog.Level(12)
const glogLevelAtomic = glog.Level(12)

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

var TxnIDCounter = sync2.NewAtomicInt64(0)

type Context map[string]float64

type WaitForTxn struct {
    *Txn
    key    string
    waiter *Txn
}

func (wft *WaitForTxn) GetKey() string {
    if wft == nil {
        return ""
    }
    return wft.key
}

func NewWaitForTxn(txn *Txn, key string, waiter *Txn) *WaitForTxn {
    return &WaitForTxn{
        Txn:    txn,
        key:    key,
        waiter: waiter,
    }
}

type AtomicWaitForTxn struct {
    waitFor *WaitForTxn
}

func NewAtomicWaitForTxn(waitFor *WaitForTxn) *AtomicWaitForTxn {
    return &AtomicWaitForTxn{
        waitFor: waitFor,
    }
}

func (at *AtomicWaitForTxn) Load() *WaitForTxn {
    pp := (*unsafe.Pointer)(unsafe.Pointer(&at.waitFor))
    return (*WaitForTxn)(atomic.LoadPointer(pp))
}

//func (at *AtomicWaitForTxn) Store(new *WaitForTxn) {
//    pp := (*unsafe.Pointer)(unsafe.Pointer(&at.waitFor))
//    atomic.StorePointer(pp, (unsafe.Pointer)(new))
//}

func (at *AtomicWaitForTxn) CompareAndSwap(old, new *WaitForTxn) (swapped bool) {
    assert.Must(old.waiter == new.waiter)

    if glog.V(glogLevelAtomic) {
        defer func() {
            if swapped {
                v := glogLevelAtomic
                if old.Txn == nil || new.Txn == nil {
                    v++
                }
                glog.V(v).Infof("txn(%d)'s waitingFor CAS from {%d} to {%d}", new.waiter.GetTimestamp(), old.GetTimestamp(), new.GetTimestamp())
            }
        } ()
    }
    return atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&at.waitFor)),
        unsafe.Pointer(old), unsafe.Pointer(new))
}

func (at *AtomicWaitForTxn) SetIf(new *WaitForTxn, pred func(old, new *WaitForTxn) bool) (swapped bool) {
    old := at.Load()
    for pred(old, new) {
        if at.CompareAndSwap(old, new) {
            return true
        }
        old = at.Load()
    }
    return
}

func (at *AtomicWaitForTxn) SetIfSameKeyAndIncr(new *WaitForTxn) (swapped bool) {
    return at.SetIf(new, func(old, new *WaitForTxn) bool {
        return new.key == old.key && new.GetTimestamp() > old.GetTimestamp()
    })
}

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

    done          chan struct{}
    wakeup        chan struct{}

    waitingListElements data_struct.ConcurrentMap
    waitingFor          *AtomicWaitForTxn
    nilWaitingFor       *WaitForTxn
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

var emptyString string

func NewTx(ops []Op) *Txn {
    txn := &Txn{
        ID:  TxnIDCounter.Add(1),
        Ops: ops,

        commitData:   make(map[string]float64),
        readVersions: make(map[string]int64),
        timestamp:    sync2.NewAtomicInt64(0),

        status:       sync2.NewAtomicInt32(int32(TxStatusInitialized)),

        firstOpMet:   false,
        ctx:          make(map[string]float64),
    }
    txn.nilWaitingFor = NewWaitForTxn(nil, "", txn)
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
    newTxn.nilWaitingFor = NewWaitForTxn(nil, "", newTxn)
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

    tx.SetStatus(TxStatusInitialized)

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

func (tx *Txn) SetStatus(status TxStatus) {
    tx.status.Set(int32(status))
}

func (tx *Txn) Start(ts *TimeServer, tid int, waitInterval time.Duration) {
    assert.Must(tx.timestamp.Get() == 0)
    assert.Must(len(tx.commitData) == 0)
    assert.Must(len(tx.readVersions) == 0)
    assert.Must(tx.GetStatus() == TxStatusInitialized)
    assert.Must(!tx.firstOpMet)

    tx.SetStatus(TxStatusPending)

    tx.waitingListElements = data_struct.NewConcurrentMap(8)
    tx.waitingFor = NewAtomicWaitForTxn(tx.nilWaitingFor)

    tx.done                = make(chan struct{})
    tx.wakeup              = make(chan struct{}, 1)

    time.Sleep(time.Duration(tid) * waitInterval)
    tx.timestamp.Set(ts.FetchTimestamp())

    glog.V(10).Infof("Start txn(%s), status: '%s'", tx.String(), tx.GetStatus().String())
}

func (tx *Txn) Done(status TxStatus) {
    assert.Must(status.Done())
    tx.SetStatus(status)

    close(tx.done)

    glog.V(4).Infof("Done txn(%s), status: '%s'", tx.String(), status.String())
}

func (tx *Txn) GetTimestamp() int64 {
    if tx == nil {
        return -1
    }
    return tx.timestamp.Get()
}

var ErrStaleRead = fmt.Errorf("stale read, should retry")

// Only waiter is owned by this thread.
func (tx *Txn) WaitFor(key string, waitingFor *Txn, holdsWriteLock bool) {
    assert.Must(!holdsWriteLock)
    var after, before *Txn
    var err error
    for {
        txWaitingListEle := waitingFor.waitingListElements.GetLazy(key, func()interface{} {
            l := data_struct.NewConcurrentList()
            ele := l.PushBack(waitingFor)
            cle := data_struct.NewConcurrentListElement(l, ele)
            return cle
        }).(*data_struct.ConcurrentListElement)

        after, _, before, _, err = insertWaiterOnKey(txWaitingListEle, key, tx, waitingFor, func(waitFor *Txn) {
            // Locked setter, otherwise later SwitchWaitingFor call may got lost due to old waitFor key is "".
            assert.Must(waitFor != nil)
            assert.Must(tx.atomicInitWaitingFor(NewWaitForTxn(waitFor, key, tx)))
        })
        if err == ErrStaleRead {
            if waitingFor.GetStatus().Done() || waitingFor.waitingFor.Load().GetKey() != key {
                break
            }
            assert.Must(false)
            continue
        }
        break
    }
    if !((before == nil || tx.GetTimestamp() > before.GetTimestamp()) && (after == nil || tx.GetTimestamp() < after.GetTimestamp())) {
        assert.Must(false)
    }

    if after != nil {
        // Long op.
        after.SwitchWaitingFor(key, NewWaitForTxn(tx, key, after))
    }
    if before != nil {
        tx.wait()
    }
    return
}

func (tx *Txn) wait() {
    waiterDesc := tx.String()

    var waited     *WaitForTxn
    waitingFor := tx.nilWaitingFor
    waitingForDesc := "NaN waitingFor"

    for {
        if waitingFor == waited && tx.atomicResetWaitingFor(waitingFor) {
            glog.V(glogLevelTxnWaitingList).Infof("txn(%s) waited txn(%s) successfully", waiterDesc, waitingForDesc)

            assert.Must(waitingFor.key != "")
            cle := tx.GetListElement(waitingFor.key)
            if cle == nil || cle.Value == nil {
                return
            }

            cl := cle.CList
            cl.Lock()
            defer cl.Unlock()
            if cle.Value == nil {
                return
            }
            gcBackwardLocked(waitingFor.key, cl.List, cle.Element)
            return
        }

        waitingFor = tx.waitingFor.Load()
        if waitingFor == nil {
            continue
        }

        assert.Must(waited == nil || waited.GetTimestamp() < waitingFor.GetTimestamp())
        waited = nil

        waitingForDesc = waitingFor.String()
        glog.V(5).Infof("txn(%s) wait for txn(%s) to finish", waiterDesc, waitingForDesc)

        select {
        case <-waitingFor.done:
            waited = waitingFor
        case <-tx.wakeup:
            waited = nil
        }
    }
}

func (tx *Txn) WaitForLegacy(waitFor *Txn) {
    assert.Must(tx.GetTimestamp() > waitFor.GetTimestamp())
    waiterDesc := tx.String()
    waitingForDesc := waitFor.String()
    glog.V(5).Infof("txn(%s) wait for txn(%s) to finish", waiterDesc, waitingForDesc)
    <-waitFor.done
    glog.V(5).Infof("txn(%s) waited txn(%s) successfully", waiterDesc, waitingForDesc)
}

func insertWaiterOnKey(begin *data_struct.ConcurrentListElement,
    key string, waiter *Txn, waitingFor *Txn, setWaitFor func (*Txn)) (after *Txn, inserted *list.Element, before *Txn,
    cleInserted *data_struct.ConcurrentListElement, err error) {
    cl := begin.CList
    l := cl.List

    cl.Lock()
    defer cl.Unlock()

    //assert.Must(l.Len() <= 1)

    if begin.Element.Value == nil {
        err = ErrStaleRead
        return
    }

    var cur, prev *list.Element

    var str string
    if glog.V(glogLevelTxnWaitingList) {
        str = fmt.Sprintf("key: '%s', list: '%s', want to insert '%d' waiting for '%d'",
            key, txnListStr(begin.CList.List), waiter.GetTimestamp(), waitingFor.GetTimestamp())
    }

    for cur, prev = begin.Element, nil; cur != nil; cur, prev = cur.Next(), cur {
        if cur.Value.(*Txn).GetTimestamp() == waiter.GetTimestamp() {
            glog.Fatalf("found same transactions '%d' in list for key '%s'", waiter.GetTimestamp(), key)
        }
        if cur.Value.(*Txn).GetTimestamp() > waiter.GetTimestamp() {
            break
        }
    }
    glog.V(glogLevelTxnWaitingList).Infof("%s, after inserted: '%s'", str, txnListStr(begin.CList.List))

    cur = gcForwardLocked(key, l, cur)
    prev = gcBackwardLocked(key, l, prev)

    if cur != nil {
        after = cur.Value.(*Txn)
        inserted = l.InsertBefore(waiter, cur)
        cleInserted = data_struct.NewConcurrentListElement(cl, inserted)
        waiter.SetListElement(key, cleInserted)
        assert.Must(cur != begin.Element)
        assert.Must(after != nil)
        assert.Must(inserted != nil)
    }
    if prev != nil {
        before = prev.Value.(*Txn)
        assert.Must(before != nil)
        if cur == nil {
            inserted = l.InsertAfter(waiter, prev)
            cleInserted = data_struct.NewConcurrentListElement(cl, inserted)
            waiter.SetListElement(key, cleInserted)
        }
        setWaitFor(before)
    }
    return
}

func gcForwardLocked(key string, l *list.List, cur *list.Element) *list.Element {
    var next *list.Element
    for cur != nil && cur.Value.(*Txn).waitingFor.Load().GetKey() != key {
        next = cur.Next()
        _ = l.Remove(cur)
        assert.Must(cur.Prev() == nil && cur.Next() == nil)
        cur.Value.(*Txn).waitingListElements.Del(key)
        glog.V(glogLevelTxnWaitingList).Infof("gcForward, deleted txn(%d) from txn list '%s'", cur.Value.(*Txn).GetTimestamp(), txnListStr(l))
        cur.Value = nil
        cur = next
    }
    return cur
}

func gcBackwardLocked(key string, l *list.List, cur *list.Element) *list.Element {
    front := l.Front()
    if front == nil {
        return nil
    }
    var prev *list.Element
    for cur != nil && ((cur == front && cur.Value.(*Txn).GetStatus().Done()) ||
        (cur != front && cur.Value.(*Txn).waitingFor.Load().GetKey() != key)) {
        prev = cur.Prev()
        _ = l.Remove(cur)
        assert.Must(cur.Prev() == nil && cur.Next() == nil)
        cur.Value.(*Txn).waitingListElements.Del(key)
        glog.V(glogLevelTxnWaitingList).Infof("gcBackward, deleted txn(%d) from txn list '%s'", cur.Value.(*Txn).GetTimestamp(), txnListStr(l))
        cur.Value = nil
        cur = prev
    }
    return cur
}

func (tx *Txn) GetListElement(key string) *data_struct.ConcurrentListElement {
    val, ok := tx.waitingListElements.Get(key)
    if !ok {
        return nil
    }
    return val.(*data_struct.ConcurrentListElement)
}

func (tx *Txn) MustGetListElement(key string) *data_struct.ConcurrentListElement {
    val, _ := tx.waitingListElements.Get(key)
    return val.(*data_struct.ConcurrentListElement)
}


func (tx *Txn) SetListElement(key string, ele *data_struct.ConcurrentListElement) {
    tx.waitingListElements.Set(key, ele)
}

func (tx *Txn) atomicInitWaitingFor(new *WaitForTxn) bool {
    return tx.waitingFor.CompareAndSwap(tx.nilWaitingFor, new)
}

func (tx *Txn) atomicResetWaitingFor(old *WaitForTxn) bool {
    return tx.waitingFor.CompareAndSwap(old, tx.nilWaitingFor)
}

func (tx *Txn) SwitchWaitingFor(key string, newWaitingFor *WaitForTxn) {
    if tx.waitingFor.SetIfSameKeyAndIncr(newWaitingFor) {
        tx.Wakeup()
    }
}

func (tx *Txn) Wakeup() {
    select {
    case tx.wakeup <-struct{}{}:
        return
    default:
        return
    }
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