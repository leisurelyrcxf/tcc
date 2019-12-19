package tcc

import (
    "fmt"
    "github.com/golang/glog"
    "tcc/expr"
)

type TxEngineBasicExecutor struct {
    db *DB
}

func NewTxEngineBasicExecutor(db *DB) *TxEngineBasicExecutor {
    return &TxEngineBasicExecutor{
        db:  db,
    }
}

func (e *TxEngineBasicExecutor) Get(key string, ctx expr.Context) (float64, error) {
    if val, ok := ctx[key]; ok {
        glog.V(10).Infof("Get cached value %f for key '%s'", val, key)
        return val, nil
    }
    val, err := e.db.Get(key)
    if err == nil {
        ctx[key] = val
        glog.V(10).Infof("Get value %f for key '%s'", val, key)
    }
    return val, err
}

func (e *TxEngineBasicExecutor) Set(key string, val float64, ctx expr.Context) error {
    e.db.SetUnsafe(key, val, 0, TxNaN)
    ctx[key] = val
    glog.V(10).Infof("Set value %f for key '%s'", val, key)
    return nil
}

type TxEngineNaive struct {
    e *TxEngineBasicExecutor
}

func NewTxEngineNaive(db *DB) *TxEngineNaive {
    return &TxEngineNaive{
        e: NewTxEngineBasicExecutor(db),
    }
}

func (te *TxEngineNaive) ExecuteTxns(db* DB, txns []*Txn) error {
    for _, tx := range txns {
        if err := te.executeSingleTx(db, tx); err != nil {
            return err
        }
    }
    return nil
}

func (te *TxEngineNaive) executeSingleTx(db* DB, tx *Txn) error {
    tx.Start(db.ts.FetchTimestamp())
    for _, op := range tx.Ops {
        if err := te.executeOp(db, tx, op); err != nil {
            return err
        }
    }
    return nil
}

func (te *TxEngineNaive) executeOp(db* DB, tx *Txn, op Op) error {
    if op.typ.IsIncr() {
        return te.executeIncrOp(db, tx, op)
    }
    if op.typ == WriteDirect {
        db.SetUnsafe(op.key, op.operatorNum, 0, tx)
        return nil
    }
    if op.typ == Procedure {
        _, err := op.expr.Eval(te.e, tx.ctx)
        return err
    }
    panic("not implemented")
}

func (te *TxEngineNaive) executeIncrOp(db* DB, tx *Txn, op Op) error {
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
    default:
        panic("not implemented")
    }
    db.SetUnsafe(op.key, val, 0, tx)
    return nil
}