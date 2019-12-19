package tcc

import (
    "fmt"
    "github.com/golang/glog"
    "sync"
    "tcc/assert"
    "tcc/data_struct"
    "time"
)

type TxEngineMVCCTO struct {
    threadNum           int
    mw                  data_struct.ConcurrentMap
    mr                  data_struct.ConcurrentMap
    lm                  *LockManager
    txns                chan *Txn
    errs                chan *TxnError
    postCommitListeners []func(*Txn)
}

func NewTxEngineMVCCTO(threadNum int, lm *LockManager) *TxEngineMVCCTO {
    return &TxEngineMVCCTO{
        threadNum:      threadNum,
        mw:             data_struct.NewConcurrentMap(1024),
        mr:             data_struct.NewConcurrentMap(1024),
        lm:             lm,
        txns:           make(chan *Txn, threadNum),
        errs:           make(chan *TxnError, threadNum * 100),
    }
}

func compactKey(key string, version int64) string {
    return fmt.Sprintf("%s\r\n%d", key, version)
}

func getMaxTxForKeyAndVersion(key string, version int64, m *data_struct.ConcurrentMap) *Txn {
    newKey := compactKey(key, version)
    if tmObj, ok := m.Get(newKey); !ok {
        return emptyTx
    } else {
        tm := tmObj.(*data_struct.ConcurrentTreeMap)
        maxKey, _ := tm.MaxIf(func(key interface{}) bool {
            return !key.(*Txn).GetStatus().HasError()
        })
        if maxKey == nil {
            return emptyTx
        }
        return maxKey.(*Txn)
    }
}

func (te *TxEngineMVCCTO) getMaxReadTxForKey(key string, version int64) *Txn {
    return getMaxTxForKeyAndVersion(key, version, &te.mr)
}

func (te *TxEngineMVCCTO) getMaxWriteTxForKey(key string, version int64) *Txn {
    return getMaxTxForKeyAndVersion(key, version, &te.mw)
}

func putTxForKeyAndVersion(key string, version int64, tx *Txn, m *data_struct.ConcurrentMap, lm *LockManager) {
    newKey := compactKey(key, version)
    tmObj, _ := m.Get(newKey)

    var tm *data_struct.ConcurrentTreeMap
    if tmObj == nil {
        lm.UpgradeLock(newKey)

        tmObj, _ = m.Get(newKey)
        if tmObj == nil {
            tm = data_struct.NewConcurrentTreeMap(func(a, b interface{}) int {
                ta := a.(*Txn)
                tb := b.(*Txn)
                return int(ta.GetTimestamp() - tb.GetTimestamp())
            })
            m.Set(newKey, tm)
        } else {
            tm = tmObj.(*data_struct.ConcurrentTreeMap)
        }

        lm.DegradeLock(newKey)
    } else {
        tm = tmObj.(*data_struct.ConcurrentTreeMap)
    }

    tm.Put(tx, nil)
}

func (te *TxEngineMVCCTO) putReadTxForKey(key string, version int64, tx *Txn, lm *LockManager) {
    putTxForKeyAndVersion(key, version, tx, &te.mr, lm)
}

func (te *TxEngineMVCCTO) putWriteTxForKey(key string, version int64, tx *Txn) {
    putTxForKeyAndVersion(key, version, tx, &te.mw, nil)
}

func removeTxForKeyAndVersion(key string, version int64, tx *Txn, m *data_struct.ConcurrentMap) {
    newKey := compactKey(key, version)
    tmObj, _ := m.Get(newKey)
    if tmObj == nil {
        return
    }
    tm := tmObj.(*data_struct.ConcurrentTreeMap)
    tm.Remove(tx)
}

func (te *TxEngineMVCCTO) removeReadTxForKey(key string, version int64, tx *Txn) {
    removeTxForKeyAndVersion(key, version, tx, &te.mr)
}

func (te *TxEngineMVCCTO) removeWriteTxForKey(key string, version int64, tx *Txn) {
    removeTxForKeyAndVersion(key, version, tx, &te.mw)
}

func (te *TxEngineMVCCTO) AddPostCommitListener(cb func(*Txn)) {
    te.postCommitListeners = append(te.postCommitListeners, cb)
}

func (te *TxEngineMVCCTO) ExecuteTxns(db *DB, txns []*Txn) error {
    go func() {
        for _, txn := range txns {
            te.txns <- txn
        }
        close(te.txns)
    }()

    var wg sync.WaitGroup
    for i := 0; i < te.threadNum; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            if err := te.executeTxnThreadFunc(db); err != nil {
                te.errs <-err.(*TxnError)
            }
        }()
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

func (te *TxEngineMVCCTO) executeTxnThreadFunc(db *DB) error {
    for txn := range te.txns {
        if err := te.executeSingleTx(db, txn); err != nil {
            return err
        }
    }
    return nil
}

func (te *TxEngineMVCCTO) executeSingleTx(db *DB, tx *Txn) error {
    for {
        err := te._executeSingleTx(db, tx)
        if err != nil && err.(*TxnError).IsRetryable() {
            tx = tx.Clone()
            continue
        }
        return err
    }
}

func (te *TxEngineMVCCTO) _executeSingleTx(db *DB, txn *Txn) (err error) {
    // Assign a new timestamp.
    txn.Start(db.ts.FetchTimestamp())

    defer func() {
        if err != nil {
            te.rollback(db, txn, err)
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

func (te *TxEngineMVCCTO) commit(db *DB, tx *Txn) {
    tx.Done(TxStatusSucceeded)

    for _, l := range te.postCommitListeners {
        l(tx)
    }
}

func (te *TxEngineMVCCTO) rollback(db *DB, tx *Txn, reason error) {
    var retryLaterStr string
    if reason.(*TxnError).IsRetryable() {
        retryLaterStr = ", retry later"
    }
    ts := tx.GetTimestamp()
    for key, _ := range tx.GetCommitData() {
        db.MustRemoveVersion(key, ts)
    }
    tx.ClearCommitData()
    tx.ClearReadVersions()
    glog.V(10).Infof("rollback txn(%s) due to error '%s'%s", tx.String(), reason.Error(), retryLaterStr)
    if reason == txnErrStaleWrite {
        tx.Done(TxStatusFailedRetryable)
    } else if reason == txnErrConflict {
        tx.Done(TxStatusFailedRetryable)
    } else {
        tx.Done(TxStatusFailed)
    }
}

func (te *TxEngineMVCCTO) executeOp(db *DB, txn *Txn, op Op) error {
    if op.typ.IsIncr() {
        return te.executeIncrOp(db, txn, op)
    }
    if op.typ == WriteDirect {
        // Needs to find out read versions still.
        _, err := te.get(db, txn, op.key)
        if err != nil {
            return err
        }
        return te.set(db, txn, op.key, op.operatorNum, db.ts)
    }
    panic("not implemented")
}

func (te *TxEngineMVCCTO) executeIncrOp(db *DB, txn *Txn, op Op) error {
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
    return te.set(db, txn, op.key, val, db.ts)
}

func (te *TxEngineMVCCTO) get(db *DB, txn *Txn, key string) (float64, error) {
    te.lm.RLock(key)
    defer te.lm.RUnlock(key)
    glog.V(10).Infof("txn(%s) want to get key '%s'", txn.String(), key)

    txn.CheckFirstOp(db.ts)
    // ts won't change because only this thread can modify it's value.
    ts := txn.GetTimestamp()

    for {
        dbVal, err := db.GetDBValueMaxVersionBelow(key, ts)
        if err != nil {
            return 0, NewTxnError(err,false)
        }
        dbValWrittenTxn := dbVal.WrittenTxn
        stats := dbValWrittenTxn.GetStatus()
        if dbValWrittenTxn == emptyTx || stats.Succeeded() {
            // TODO remove this
            assert.Must(dbVal.WrittenTxn.GetTimestamp() == dbVal.Version)
            te.putReadTxForKey(key, dbVal.Version, txn, db.lm)
            glog.V(10).Infof("txn(%s) got value %f for key '%s'", txn.String(), dbVal.Value, key)
            return dbVal.Value, nil
        }
        // TODO Try return directly, for readonly wait, otherwise return error.
        if stats.HasError() {
            continue
        }
        db.lm.RUnlock(key)
        dbValWrittenTxn.WaitUntilDone(txn.String())
        db.lm.RLock(key)
    }
}

func (te *TxEngineMVCCTO) set(db *DB, txn *Txn, key string, val float64, timeServ *TimeServer) error {
    te.lm.Lock(key)
    defer te.lm.Unlock(key)
    glog.V(10).Infof("txn(%s) want to set key '%s' to value %f", txn.String(), key, val)

    txn.CheckFirstOp(timeServ)
    ts := txn.GetTimestamp()

    // Write-read conflict
    if ts < te.getMaxReadTxForKey(key, 0).GetTimestamp() {
        return txnErrConflict
    }

    // Write-write conflict
    for {
        maxWriteTxn := te.getMaxWriteTxForKey(key, 0)
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
    te.putWriteTxForKey(key, 0, txn)
    glog.V(10).Infof("txn(%s) succeeded in setting key '%s' to value %f", txn.String(), key, val)
    return nil
}