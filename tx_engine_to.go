package tcc

import (
    "fmt"
    "github.com/golang/glog"
    "sync"
    "tcc/assert"
    "tcc/data_struct"
    "tcc/expr"
    "time"
)

var txnErrConflict = NewTxnError(fmt.Errorf("txn conflict"), true)
var txnErrStaleWrite = NewTxnError(fmt.Errorf("stale write"), true)

type TxEngineExecutorTO struct {
    te          *TxEngineTO
    db          *DB
}

func NewTxEngineExecutorTO(te *TxEngineTO, db* DB) *TxEngineExecutorTO {
    return &TxEngineExecutorTO{
        te:           te,
        db:           db,
    }
}

func (e *TxEngineExecutorTO) Get(key string, ctx expr.Context) (float64, error) {
    return e.te.get(e.db, ctx.(*Txn), key)
}

func (e *TxEngineExecutorTO) Set(key string, val float64, ctx expr.Context) error {
    return e.te.set(ctx.(*Txn), key, val, e.db.ts)
}

type TxEngineTO struct {
    threadNum           int
    mw                  data_struct.ConcurrentMap
    mr                  data_struct.ConcurrentMap
    lm                  *LockManager
    txns                chan *Txn
    errs                chan *TxnError
    postCommitListeners []func(*Txn)
    retryWaitInterval   time.Duration
    e                   *TxEngineExecutorTO
}

func NewTxEngineTO(db *DB, threadNum int, lm *LockManager, retryWaitInterval time.Duration) *TxEngineTO {
    te := &TxEngineTO{
        threadNum:         threadNum,
        mw:                data_struct.NewConcurrentMap(1024),
        mr:                data_struct.NewConcurrentMap(1024),
        lm:                lm,
        txns:              make(chan *Txn, threadNum),
        errs:              make(chan *TxnError, threadNum * 100),
        retryWaitInterval: retryWaitInterval,
    }
    te.e = NewTxEngineExecutorTO(te, db)
    return te
}

func getMaxTxForKey(key string, m *data_struct.ConcurrentMap) *Txn {
    if tmObj, ok := m.Get(key); !ok {
        return TxNaN
    } else {
        tm := tmObj.(*data_struct.ConcurrentTreeMap)
        maxKey, _ := tm.MaxIf(func(key interface{}) bool {
            return !key.(*Txn).GetStatus().HasError()
        })
        if maxKey == nil {
            return TxNaN
        }
        return maxKey.(*Txn)
    }
}

func (te *TxEngineTO) getMaxReadTxForKey(key string) *Txn {
    return getMaxTxForKey(key, &te.mr)
}

func (te *TxEngineTO) getMaxWriteTxForKey(key string) *Txn {
    return getMaxTxForKey(key, &te.mw)
}

func putTxForKey(key string, tx *Txn, m *data_struct.ConcurrentMap, holdsWriteLock bool) {
    if !holdsWriteLock {
        m.GetLazy(key, func()interface{} {
            return data_struct.NewConcurrentTreeMap(func(a, b interface{}) int {
                ta := a.(*Txn)
                tb := b.(*Txn)
                return int(ta.GetTimestamp() - tb.GetTimestamp())
            })
        }).(*data_struct.ConcurrentTreeMap).Put(tx, nil)
        return
    }

    var tm *data_struct.ConcurrentTreeMap
    tmObj, ok := m.Get(key)
    if !ok {
        tm = data_struct.NewConcurrentTreeMap(func(a, b interface{}) int {
            ta := a.(*Txn)
            tb := b.(*Txn)
            return int(ta.GetTimestamp() - tb.GetTimestamp())
        })
        m.Set(key, tm)
    } else {
        tm = tmObj.(*data_struct.ConcurrentTreeMap)
    }
    tm.Put(tx, nil)
}

func (te *TxEngineTO) putReadTxForKey(key string, tx *Txn) {
    putTxForKey(key, tx, &te.mr, false)
}

func (te *TxEngineTO) putWriteTxForKey(key string, tx *Txn) {
    putTxForKey(key, tx, &te.mw, true)
}

func (te *TxEngineTO) AddPostCommitListener(cb func(*Txn)) {
    te.postCommitListeners = append(te.postCommitListeners, cb)
}

func (te *TxEngineTO) ExecuteTxns(db *DB, txns []*Txn) error {
    go func() {
        for _, txn := range txns {
            te.txns <- txn
        }
        close(te.txns)
    }()

    var wg sync.WaitGroup
    for i := 0; i < te.threadNum; i++ {
        wg.Add(1)
        go func(tid int) {
            defer wg.Done()
            if err := te.executeTxnsSingleThread(db, tid); err != nil {
                te.errs <-err.(*TxnError)
            }
        }(i)
    }

    // Wait txn threads to end.
    wg.Wait()

    select {
    case err := <- te.errs:
        return err
    default:
        return nil
    }
}

func (te *TxEngineTO) executeTxnsSingleThread(db *DB, tid int) error {
    for txn := range te.txns {
        if err := te.executeSingleTx(db, txn, tid); err != nil {
            return err
        }
    }
    return nil
}

func (te *TxEngineTO) executeSingleTx(db *DB, tx *Txn, tid int) error {
    var startWaitInterval time.Duration
    for {
        err := te._executeSingleTx(db, tx, tid, startWaitInterval)
        if err != nil && err.(*TxnError).IsRetryable() {
            tx = tx.Clone()
            startWaitInterval = te.retryWaitInterval
            continue
        }
        return err
    }
}

func (te *TxEngineTO) _executeSingleTx(db *DB, txn *Txn, tid int, startWaitInterval time.Duration) (err error) {
    // Assign a new timestamp.
    txn.Start(db.ts, tid, startWaitInterval)

    defer func() {
        if err != nil {
            te.rollback(txn, err)
        } else {
            te.commit(db, txn)
        }
    }()

    for _, op := range txn.Ops {
        if err = te.executeOp(db, txn, op); err != nil {
            return
        }
    }
    return
}

func (te *TxEngineTO) commit(db *DB, tx *Txn) {
    for key, val := range tx.GetCommitData() {
        _ = db.SetSafe(key, val, tx)
    }
    tx.Done(TxStatusSucceeded)
    for _, l := range te.postCommitListeners {
        l(tx)
    }
}

func (te *TxEngineTO) rollback(tx *Txn, reason error) {
    var retryLaterStr string
    if reason.(*TxnError).IsRetryable() {
        retryLaterStr = ", retry later"
    }
    glog.V(10).Infof("rollback txn(%s) due to error '%s'%s", tx.String(), reason.Error(), retryLaterStr)
    //for _, key := range tx.CollectKeys() {
    //    te.removeReadTxForKey(key, tx)
    //    te.removeWriteTxForKey(key, tx)
    //}
    tx.Clear()
    if reason == txnErrStaleWrite {
        tx.Done(TxStatusFailedRetryable)
    } else if reason == txnErrConflict {
        tx.Done(TxStatusFailedRetryable)
    } else {
        tx.Done(TxStatusFailed)
    }
}

func (te *TxEngineTO) executeOp(db *DB, txn *Txn, op Op) error {
    if op.typ.IsIncr() {
        return te.executeIncrOp(db, txn, op)
    }
    if op.typ == WriteDirect {
        return te.set(txn, op.key, op.operatorNum, db.ts)
    }
    if op.typ == Procedure {
        _, err := op.expr.Eval(te.e, txn)
        return err
    }
    panic("not implemented")
}

func (te *TxEngineTO) executeIncrOp(db *DB, txn *Txn, op Op) error {
    val, err := te.get(db, txn, op.key)
    if err != nil {
        return err
    }
    switch op.typ {
    case IncrMinus:
        val -= op.operatorNum
    case IncrAdd:
        val += op.operatorNum
    case IncrMultiply:
        val *= op.operatorNum
    default:
        panic("unimplemented")
    }
    return te.set(txn, op.key, val, db.ts)
}

func (te *TxEngineTO) get(db *DB, txn *Txn, key string) (float64, error) {
    te.lm.RLock(key)
    defer te.lm.RUnlock(key)
    glog.V(10).Infof("txn(%s) want to get key '%s'", txn.String(), key)

    txn.CheckFirstOp(db.ts)
    ts := txn.GetTimestamp()

    for {
        maxWriteTxn := te.getMaxWriteTxForKey(key)
        // maxWriteTxnTs is only a snapshot may change any time later.
        maxWriteTxnTs := maxWriteTxn.GetTimestamp()
        if maxWriteTxn == TxNaN || maxWriteTxnTs == ts {
            // Empty or is self, always safe.
            assert.Must(maxWriteTxn == TxNaN || maxWriteTxn == txn)
            break
        }

        if maxWriteTxnTs > ts  {
            // Read-write conflict.
            // If we can't read later version, then it's still safe.
            // Let's check later.
            break
        }

        // assert.Must(maxWriteTxnTs < ts)

        // mwStatus is also a snapshot
        mwStatus := maxWriteTxn.GetStatus()
        if mwStatus == TxStatusSucceeded {
            // Already succeeded, safe property could be proved as below:
            // For such tx which holds maxWriteTxn.Timestamp < tx.Timestamp < this_txn.Timestamp:
            //     If tx has already written to key, then it's a contradiction (maxWriteTxn will be tx instead);
            //     if tx has not written to the key, it will never have a chance to write to the key in the future
            //         because it will find tx.Timestamp < maxReadTxn.Timestamp
            //         (maxReadTxn.Timestamp >= this_txn.Timestamp).
            //
            // For such tx which holds tx.Timestamp < maxWriteTxn.Timestamp < this_txn.Timestamp, since
            // maxWriteTxn's write to the key has already been saved to db, tx's write to the key will
            // never be visible.
            //     If it has already written, then the value has been overwritten by maxWriteTxn;
            //     if it has not written, it will be either omitted (Thomas' write rule),
            //     either be discarded (if Thomas's write rule is not applied.
            break
        }
        if mwStatus.HasError() {
            // Already failed, then this maxWriteTxn must have been rollbacked, retry.
            continue
        }

        db.lm.RUnlock(key)
        txn.WaitForBasic(maxWriteTxn)
        db.lm.RLock(key)
    }

    vv, dbErr := db.GetDBValue(key)
    if dbErr != nil {
        return 0, NewTxnError(dbErr, false)
    }

    if ts < vv.Version {
        // Read future versions, can't do anything
        return 0, txnErrConflict
    }
    // No need to check cause if we read that version, it is not possible to rollback.
    //writtenTxn := vv.WrittenTxn
    //if writtenTxn != nil && !writtenTxn.GetStatus().Done() {
    //    writtenTxn.txn(txn)
    //}
    te.putReadTxForKey(key, txn)
    glog.V(10).Infof("txn(%s) got value %f for key '%s'", txn.String(), vv.Value, key)
    return vv.Value, nil
}

func (te *TxEngineTO) set(txn *Txn, key string, val float64, timeServ *TimeServer) error {
    te.lm.Lock(key)
    defer te.lm.Unlock(key)
    glog.V(10).Infof("txn(%s) want to set key '%s' to value %f", txn.String(), key, val)

    txn.CheckFirstOp(timeServ)
    ts := txn.GetTimestamp()

    // Write-read conflict
    if ts < te.getMaxReadTxForKey(key).GetTimestamp() {
        return txnErrConflict
    }

    // Write-write conflict
    for {
        maxWriteTxn := te.getMaxWriteTxForKey(key)
        if ts < maxWriteTxn.GetTimestamp() {
            for i := 0; i < 8; i++ {
                status := maxWriteTxn.GetStatus()
                if status.Done() {
                    if status.Succeeded() {
                        // Apply Thomas's write rule.
                        return nil
                    }
                    //assert.Must(status.HasError())
                    break
                }
                time.Sleep(1 * time.Millisecond)
            }
            if maxWriteTxn.GetStatus().HasError() {
                continue
            }
            // Since maxWriteTxn's status is not known, it could rollback later,
            // ellipsis not safe here.
            return txnErrStaleWrite
        } else {
            break
        }
    }

    txn.AddCommitData(key, val)
    te.putWriteTxForKey(key, txn)
    glog.V(10).Infof("txn(%s) succeeded in setting key '%s' to value %f", txn.String(), key, val)
    return nil
}