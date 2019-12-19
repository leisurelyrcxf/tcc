package tcc

import (
    "fmt"
    "tcc/expr"
)

type TxEngineNaiveExecutor struct {
    db *DB
    ctx expr.Context
}

func NewTxEngineNaiveExecutor(db *DB) *TxEngineNaiveExecutor {
    return &TxEngineNaiveExecutor{
        db:  db,
        ctx: nil,
    }
}

func (e *TxEngineNaiveExecutor) Get(key string) (float64, error) {
    return e.db.Get(key)
}

func (e *TxEngineNaiveExecutor) Set(key string, val float64) error {
    e.db.SetUnsafe(key, val, 0, TxNaN)
    return nil
}

func (e *TxEngineNaiveExecutor) GetContext() expr.Context {
    return e.ctx
}

type TxEngineNaive struct {
    e *TxEngineNaiveExecutor
}

func NewTxEngineNaive(db *DB) *TxEngineNaive {
    return &TxEngineNaive{
        e: NewTxEngineNaiveExecutor(db),
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
    te.e.ctx = tx.ctx
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
        _, err := op.expr.Eval(te.e)
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