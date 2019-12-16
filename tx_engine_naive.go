package ts_promote

import "fmt"

type TxEngineNaive struct {

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