package tcc

import (
    "fmt"
    "sort"
    "sync"
)

type TxEngineC2PL struct {
    threadNum           int
    txns                chan *Txn
    errs                chan *TxnError
    e                   *TxEngineBasicExecutor
    postCommitListeners []func(*Txn)
}

func NewTxEngineC2PL(db *DB, threadNum int) *TxEngineC2PL {
    return &TxEngineC2PL{
        threadNum:         threadNum,
        txns:              make(chan *Txn, threadNum),
        errs:              make(chan *TxnError, threadNum),
        e:                 NewTxEngineBasicExecutor(db),
    }
}

func (te *TxEngineC2PL) AddPostCommitListener(cb func(*Txn)) {
    te.postCommitListeners = append(te.postCommitListeners, cb)
}

func (te *TxEngineC2PL) ExecuteTxns(db* DB, txns []*Txn) error {
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
                te.errs <- err.(*TxnError)
            }
        }(i)
    }
    wg.Wait()

    select {
    case err := <- te.errs:
        return err
    default:
        return nil
    }
}

func (te *TxEngineC2PL) executeTxnsSingleThread(db* DB, tid int) error {
    for txn := range te.txns {
        if err := te.executeSingleTx(db, txn, tid); err != nil {
            return err
        }
    }
    return nil
}

func (te *TxEngineC2PL) executeSingleTx(db* DB, tx *Txn, tid int) error {
    tx.Start(db.ts, tid, 0)

    keys := tx.CollectKeys()
    sort.Strings(keys)
    for _, key := range keys {
        db.lm.Lock(key)
    }
    defer func() {
        for i := len(keys) - 1; i >= 0; i-- {
            db.lm.Unlock(keys[i])
        }
    }()

    for _, op := range tx.Ops {
        if err := te.executeOp(db, tx, op); err != nil {
            return err
        }
    }

    for _, l := range te.postCommitListeners {
        l(tx)
    }
    return nil
}

func (te *TxEngineC2PL) executeOp(db* DB, tx *Txn, op Op) error {
    if op.typ.IsIncr() {
        return te.executeIncrOp(db, tx, op)
    }
    if op.typ == WriteDirect {
        db.SetUnsafe(op.key, op.operatorNum, 0, tx)
        return nil
    }
    if op.typ == Procedure {
        _, err := op.expr.Eval(te.e, tx)
        return err
    }
    panic("not implemented")
}

func (te *TxEngineC2PL) executeIncrOp(db* DB, tx *Txn, op Op) error {
    val, err := db.Get(op.key)
    if err != nil {
        return NewTxnError(fmt.Errorf("key '%s' not exist, detail: '%s'", op.key, err), false)
    }
    switch op.typ {
    case IncrMinus:
        val -= op.operatorNum
    case IncrAdd:
        val += op.operatorNum
    case IncrMultiply:
        val *= op.operatorNum
    }
    db.SetUnsafe(op.key, val, 0, tx)
    return nil
}