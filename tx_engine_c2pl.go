package ts_promote

import (
    "fmt"
    "sort"
    "sync"
)

type TxEngineC2PL struct {
    threadNum int
    txns chan *Tx
    errs chan error
}

func NewTxEngineC2PL(threadNum int) *TxEngineC2PL {
    return &TxEngineC2PL{
        threadNum: threadNum,
        txns: make(chan *Tx, threadNum),
        errs: make(chan error, threadNum),
    }
}

func (te *TxEngineC2PL) ExecuteTxns(db* DB, txns []*Tx) error {
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
            if err := te.executeTxns(db); err != nil {
                te.errs <- err
            }
        }()
    }
    wg.Wait()

    select {
    case err := <- te.errs:
        return err
    default:
        return nil
    }
}

func (te *TxEngineC2PL) executeTxns(db* DB) error {
    for txn := range te.txns {
        txn.Timestamp = db.ts.FetchTimestamp()
        if err := te.executeSingleTx(db, txn); err != nil {
            return err
        }
    }
    return nil
}

func (te *TxEngineC2PL) executeSingleTx(db* DB, tx *Tx) error {
    keys := tx.CollectKeys()
    sort.Strings(keys)
    for _, key := range keys {
        db.lm.lockKey(key)
    }
    defer func() {
        for _, key := range keys {
            db.lm.unlockKey(key)
        }
    }()

    for _, op := range tx.Ops {
        if err := te.executeOp(db, op); err != nil {
            return err
        }
    }
    return nil
}

func (te *TxEngineC2PL) executeOp(db* DB, op Op) error {
    val, err := db.get(op.key)
    if err != nil {
        return fmt.Errorf("key '%s' not exist, detail: '%s'", op.key, err)
    }
    switch op.typ {
    case IncrMinus:
        val -= op.operatorNum
    case IncrAdd:
        val += op.operatorNum
    case IncrMultiply:
        val *= op.operatorNum
    }
    db.set(op.key, val)
    return nil
}

func (te *TxEngineC2PL) get(db* DB, key string) (float64, error) {
    db.lm.lockKey(key)
    defer db.lm.unlockKey(key)
    return db.get(key)
}

func (te *TxEngineC2PL) set(db* DB, key string, val float64) {
    db.lm.lockKey(key)
    defer db.lm.unlockKey(key)
    db.set(key, val)
}