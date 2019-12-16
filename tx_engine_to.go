package ts_promote

import (
    "fmt"
    "github.com/golang/glog"
    "sync"
    "ts_promote/assert"
    "ts_promote/data_struct"
)

type TxEngineTO struct {
    threadNum      int
    mw             data_struct.ConcurrentMap
    mr             data_struct.ConcurrentMap
    lm             *LockManager
    txns           chan *Txn
    committingTxns chan *Txn
    committerDone  chan struct{}
    errs           chan *TxnError
}

func NewTxEngineTO(threadNum int, lm *LockManager) *TxEngineTO {
    return &TxEngineTO{
        threadNum:      threadNum,
        mw:             data_struct.NewConcurrentMap(1024),
        mr:             data_struct.NewConcurrentMap(1024),
        lm:             lm,
        txns:           make(chan *Txn, threadNum),
        committingTxns: make(chan *Txn, 100),
        committerDone:  make(chan struct{}),
        errs:           make(chan *TxnError, threadNum * 100),
    }
}

var txConflictErr = NewTxnError(fmt.Errorf("txn conflict"), true)
var txStaleWrite = NewTxnError(fmt.Errorf("stale write"), true)

func getMaxTxForKey(key string, m *data_struct.ConcurrentMap) *Txn {
    if obj, ok := m.Get(key); !ok {
        return emptyTx
    } else {
        tm := obj.(*data_struct.ConcurrentTreeMap)

        obj, _ = tm.Max()
        if obj == nil {
            return emptyTx
        }
        return obj.(*Txn)
    }
}

func (te *TxEngineTO) getMaxReadTxForKey(key string) *Txn {
    return getMaxTxForKey(key, &te.mr)
}

func (te *TxEngineTO) getMaxWriteTxForKey(key string) *Txn {
    return getMaxTxForKey(key, &te.mw)
}

func putTxForKey(key string, tx *Txn, m *data_struct.ConcurrentMap) {
    obj, _ := m.Get(key)

    var tm *data_struct.ConcurrentTreeMap
    if obj == nil {
        tm = data_struct.NewConcurrentTreeMap(func(a, b interface{}) int {
            ta := a.(*Txn)
            tb := b.(*Txn)
            return int(ta.GetTimestamp() - tb.GetTimestamp())
        })
        m.Set(key, tm)
    } else {
        tm = obj.(*data_struct.ConcurrentTreeMap)
    }

    tm.Put(tx, nil)
}

func (te *TxEngineTO) putReadTxForKey(key string, tx *Txn) {
    putTxForKey(key, tx, &te.mr)
}

func (te *TxEngineTO) putWriteTxForKey(key string, tx *Txn) {
    putTxForKey(key, tx, &te.mw)
}

func removeTxForKey(key string, tx *Txn, m *data_struct.ConcurrentMap) {
    obj, _ := m.Get(key)
    if obj == nil {
        return
    }
    tm := obj.(*data_struct.ConcurrentTreeMap)
    tm.Remove(tx)
}

func (te *TxEngineTO) removeReadTxForKey(key string, tx *Txn) {
    removeTxForKey(key, tx, &te.mr)
}

func (te *TxEngineTO) removeWriteTxForKey(key string, tx *Txn) {
    removeTxForKey(key, tx, &te.mw)
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
        go func() {
            defer wg.Done()
            if err := te.executeTxnsSingleThread(db); err != nil {
                te.errs <-err.(*TxnError)
            }
        }()
    }

    go te.committer(db)

    // Wait txn threads to end.
    wg.Wait()
    // Notify committer to end.
    close(te.committingTxns)
    // Wait committer to end.
    <-te.committerDone

    select {
    case err := <- te.errs:
        return err
    default:
        return nil
    }
}

func (te *TxEngineTO) executeTxnsSingleThread(db *DB) error {
    for txn := range te.txns {
        if err := te.executeSingleTx(db, txn); err != nil {
            return err
        }
    }
    return nil
}

func (te *TxEngineTO) committer(db *DB) {
    defer close(te.committerDone)
    for txn := range te.committingTxns {
        ts := txn.GetTimestamp()
        for k, v := range txn.GetCommitData() {
            vv, err := db.GetVersionedValue(k)
            if err == KeyNotExist {
                // Safe here.
                db.SetUnsafe(k, v, ts)
                continue
            }
            if err != nil {
                te.errs <-NewTxnError(fmt.Errorf("unexpected happens during db.Get, detail: '%s'", err.Error()), false)
                txn.Done(TxStatusFailed)
                return
            }
            if ts >= vv.Version {
                // Safe here.
                db.SetUnsafe(k, v, ts)
            } else {
                msg := fmt.Sprintf("ignored txn(%d, %d) committed value %f for key '%s'," +
                    " version_num_of_txn(%d) < committed_version(%d)",
                    txn.TxId, ts, v, k, ts, vv.Version)
                glog.Warningf(msg)
                panic(msg)
            }
        }
        glog.Infof("txn(%s) succeeded", txn.String())
        txn.Done(TxStatusSucceeded)
    }
}

func (te *TxEngineTO) executeSingleTx(db *DB, tx *Txn) error {
    for {
        err := te._executeSingleTx(db, tx)
        if err != nil && err.(*TxnError).IsRetryable() {
            tx.ReInit()
            continue
        }
        return err
    }
}

func (te *TxEngineTO) _executeSingleTx(db *DB, txn *Txn) (err error) {
    // Assign a new timestamp.
    txn.SetTimestamp(db.ts.FetchTimestamp())

    defer func() {
        if err != nil {
            te.rollback(txn, err)
        }
    }()

    for _, op := range txn.Ops {
        if err = te.executeOp(db, txn, op); err != nil {
            return
        }
    }

    te.committingTxns <- txn
    return
}

func (te *TxEngineTO) rollback(tx *Txn, reason error) {
    var retryLaterStr string
    if reason.(*TxnError).IsRetryable() {
        retryLaterStr = ", retry later"
    }
    glog.Infof("rollback txn(%s) due to error '%s'%s", tx.String(), reason.Error(), retryLaterStr)
    for _, key := range tx.CollectKeys() {
        te.removeReadTxForKey(key, tx)
        te.removeWriteTxForKey(key, tx)
    }
    tx.ClearCommitData()
    if reason == txStaleWrite {
        tx.Done(TxStatusFailedRetryable)
    } else if reason == txConflictErr {
        tx.Done(TxStatusFailedRetryable)
    } else {
        tx.Done(TxStatusFailed)
    }
}

func (te *TxEngineTO) executeOp(db *DB, txn *Txn, op Op) error {
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
    }
    return te.set(txn, op.key, val)
}

func (te *TxEngineTO) get(db *DB, txn *Txn, key string) (val float64, err error) {
    te.lm.lockKey(key)
    defer te.lm.unlockKey(key)

    glog.Infof("txn(%s) want to get key '%s'", txn.String(), key)
    // ts won't change because only this thread can modify it's value.
    ts := txn.GetTimestamp()

    for {
        maxWriteTxn := te.getMaxWriteTxForKey(key)
        // round is also a snapshot
        round := maxWriteTxn.GetRound()
        // maxWriteTxnTs is only a snapshot may change any time later.
        maxWriteTxnTs := maxWriteTxn.GetTimestamp()
        if maxWriteTxn == emptyTx || maxWriteTxnTs == ts {
            // Empty or is self, always safe.
            assert.Must(maxWriteTxn == emptyTx || maxWriteTxn == txn)
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
        if mwStatus == TxStatusFailedRetryable || mwStatus == TxStatusFailed {
            // Already failed, then this maxWriteTxn must have been rollbacked
            continue
        }

        // Wait until depending txn finishes.
        if maxWriteTxn.GetRound() != round  ||
            maxWriteTxn.GetStatus().Done() ||
            maxWriteTxn.GetTimestamp() != maxWriteTxnTs {
           continue
        }
        db.lm.unlockKey(key)
        maxWriteTxn.WaitTillDone(round, txn)
        db.lm.lockKey(key)
    }

    vv, dbErr := db.GetVersionedValue(key)
    if dbErr != nil {
        err = NewTxnError(dbErr, false)
        return
    }
    if ts < vv.Version {
        // Read future versions, can't do anything
        err = txConflictErr
        return
    }
    te.putReadTxForKey(key, Later(te.getMaxReadTxForKey(key), txn))
    val = vv.Value
    return
}

func (te *TxEngineTO) set(txn *Txn, key string, val float64) (err error) {
    te.lm.lockKey(key)
    defer te.lm.unlockKey(key)
    glog.Infof("txn(%s) want to set key '%s' to value %f", txn.String(), key, val)

    ts := txn.GetTimestamp()

    // Write-read conflict
    if ts < te.getMaxReadTxForKey(key).GetTimestamp() {
        err = txConflictErr
        return
    }

    // Write-write conflict
    // TODO Thomas's ellison rule.
    if ts < te.getMaxWriteTxForKey(key).GetTimestamp() {
        err = txStaleWrite
        return
    }

    txn.AddCommitData(key, val)
    te.putWriteTxForKey(key, txn)
    return
}