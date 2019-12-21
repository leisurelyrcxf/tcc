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

type TxEngineExecutorMVCCTO struct {
    te          *TxEngineMVCCTO
    db          *DB
    enableCache bool
}

func NewTxEngineExecutorMVCCTO(te *TxEngineMVCCTO, db* DB, enableCache bool) *TxEngineExecutorMVCCTO {
    return &TxEngineExecutorMVCCTO{
        te:           te,
        db:           db,
        enableCache:  enableCache,
    }
}

func (e *TxEngineExecutorMVCCTO) Get(key string, ctx expr.Context) (float64, error) {
    if !e.enableCache {
        return e.te.get(e.db, ctx.(*Txn), key)
    }
    tx := ctx.(*Txn)
    if val, ok := tx.ctx[key]; ok {
        glog.V(10).Infof("Get cached value %f for key '%s'", val, key)
        return val, nil
    }
    val, err := e.te.get(e.db, tx, key)
    if err == nil {
        tx.ctx[key] = val
        glog.V(10).Infof("Get value %f for key '%s'", val, key)
    }
    return val, err
}

func (e *TxEngineExecutorMVCCTO) Set(key string, val float64, ctx expr.Context) error {
    tx := ctx.(*Txn)
    delete(tx.ctx, key)
    err := e.te.set(e.db, tx, key, val)
    glog.V(10).Infof("Set value %f for key '%s'", val, key)
    return err
}

type TxEngineMVCCTO struct {
    threadNum           int
    mr                  data_struct.ConcurrentMap
    txns                chan *Txn
    errs                chan *TxnError
    retryWaitInterval   time.Duration
    postCommitListeners []func(*Txn)
    e                   *TxEngineExecutorMVCCTO
}

func NewTxEngineMVCCTO(db *DB, threadNum int, enableCache bool, retryWaitInterval time.Duration) *TxEngineMVCCTO {
    te := &TxEngineMVCCTO{
        threadNum:         threadNum,
        mr:                data_struct.NewConcurrentMap(1024),
        txns:              make(chan *Txn, threadNum),
        retryWaitInterval: retryWaitInterval,
        errs:              make(chan *TxnError, threadNum * 100),
    }
    te.e = NewTxEngineExecutorMVCCTO(te, db, enableCache)
    return te
}

func compactKey(key string, version int64) string {
    return fmt.Sprintf("%s\r\n%d", key, version)
}

func (te *TxEngineMVCCTO) getMaxReadTxForKeyAndVersion(key string, version int64) *Txn {
    newKey := compactKey(key, version)
    if tmObj, ok := te.mr.Get(newKey); !ok {
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

func (te *TxEngineMVCCTO) putReadTxForKeyAndVersion(key string, version int64, tx *Txn) {
    newKey := compactKey(key, version)
    te.mr.GetLazy(newKey, func()interface{} {
        return data_struct.NewConcurrentTreeMap(func(a, b interface{}) int {
            ta := a.(*Txn)
            tb := b.(*Txn)
            return int(ta.GetTimestamp() - tb.GetTimestamp())
        })
    }).(*data_struct.ConcurrentTreeMap).Put(tx, nil)
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
        go func(tid int) {
            defer wg.Done()
            if err := te.executeTxnThreadFunc(db, tid); err != nil {
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

func (te *TxEngineMVCCTO) executeTxnThreadFunc(db *DB, tid int) error {
    for txn := range te.txns {
        if err := te.executeSingleTx(db, txn, tid); err != nil {
            return err
        }
    }
    return nil
}

func (te *TxEngineMVCCTO) executeSingleTx(db *DB, tx *Txn, tid int) error {
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

func (te *TxEngineMVCCTO) _executeSingleTx(db *DB, txn *Txn, tid int, startWaitInterval time.Duration) (err error) {
    // Assign a new timestamp.
    txn.Start(db.ts, tid, startWaitInterval)

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

func (te *TxEngineMVCCTO) commit(db *DB, txn *Txn) {
    txn.Done(TxStatusSucceeded)

    for _, l := range te.postCommitListeners {
        l(txn)
    }
}

func (te *TxEngineMVCCTO) rollback(db *DB, txn *Txn, reason error) {
    if glog.V(10) {
        var retryLaterStr string
        if reason.(*TxnError).IsRetryable() {
            retryLaterStr = ", retry later"
        }
        glog.Infof("rollback txn(%s) due to error '%s'%s", txn.String(), reason.Error(), retryLaterStr)
    }
    ts := txn.GetTimestamp()
    for key, _ := range txn.GetCommitData() {
        db.MustRemoveVersion(key, ts)
    }
    txn.Clear()
    if reason == txnErrStaleWrite {
        txn.Done(TxStatusFailedRetryable)
    } else if reason == txnErrConflict {
        txn.Done(TxStatusFailedRetryable)
    } else {
        txn.Done(TxStatusFailed)
    }
}

func (te *TxEngineMVCCTO) executeOp(db *DB, txn *Txn, op Op) error {
    if op.typ.IsIncr() {
        return te.executeIncrOp(db, txn, op)
    }
    if op.typ == WriteDirect {
        // Needs to find out read versions still.
        return te.set(db, txn, op.key, op.operatorNum)
    }
    if op.typ == Procedure {
        _, err := op.expr.Eval(te.e, txn)
        if err != nil {
            return NewTxnError(fmt.Errorf("error executing procudure, detail: '%s'", err.Error()), false)
        }
        return nil
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
    return te.set(db, txn, op.key, val)
}

func (te *TxEngineMVCCTO) get(db *DB, txn *Txn, key string) (float64, error) {
    txn.CheckFirstOp(db.ts)
    ts := txn.GetTimestamp()
    glog.V(10).Infof("txn(%s) want to get key '%s'", txn.String(), key)

    db.lm.RLock(key)
    locked := true
    defer func() {
        if locked { db.lm.RUnlock(key) }
    }()

    for {
        dbVal, err := db.GetDBValueMaxVersionBelow(key, ts)
        if err != nil {
            return 0, NewTxnError(err,false)
        }

        dbValWrittenTxn := dbVal.WrittenTxn
        assert.Must(dbValWrittenTxn.GetTimestamp() == dbVal.Version)
        stats := dbValWrittenTxn.GetStatus()

        if dbValWrittenTxn == TxNaN || stats.Succeeded() {
            te.putReadTxForKeyAndVersion(key, dbVal.Version, txn)
            txn.AddReadVersion(key, dbVal.Version)
            glog.V(10).Infof("txn(%s) got value(%f, %d) for key '%s'", txn.String(), dbVal.Value, dbVal.Version, key)
            return dbVal.Value, nil
        }

        if stats.HasError() {
            continue
        }

        //return 0, txnErrConflict

        db.lm.RUnlock(key)
        locked = false

        txn.WaitFor(key, dbValWrittenTxn, false)
        //txn.WaitForLegacy(dbValWrittenTxn)

        db.lm.RLock(key)
        locked = true
    }
}

func (te *TxEngineMVCCTO) set(db *DB, txn *Txn, key string, val float64) error {
    txn.CheckFirstOp(db.ts)
    ts := txn.GetTimestamp()
    glog.V(10).Infof("txn(%s) want to set key '%s' to value(%f, %d)", txn.String(), key, val, ts)

    readVersion, readVersionExist := txn.readVersions[key]

    db.lm.Lock(key)
    defer db.lm.Unlock(key)

    if !readVersionExist {
        dbVal, err := db.GetDBValueMaxVersionBelow(key, ts)
        if err != nil {
            return NewTxnError(err,false)
        }
        readVersion = dbVal.Version
    } else {
        //dbVal, err := db.GetDBValueMaxVersionBelow(key, ts)
        //assert.MustNoError(err)
        //assert.Must(dbVal.Version == readVersion)
    }

    // Write-read conflict
    if ts < te.getMaxReadTxForKeyAndVersion(key, readVersion).GetTimestamp() {
        return txnErrConflict
    }

    // Write-write conflict
    txn.AddCommitData(key, val)
    db.SetMVCC(key, val, txn, true)
    glog.V(10).Infof("txn(%s) succeeded in setting key '%s' to value %f", txn.String(), key, val)
    return nil
}