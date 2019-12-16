package ts_promote

import "fmt"

type TxEngineNaive struct {

}

func (te *TxEngineNaive) ExecuteTxns(db* DB, txns []*Tx) error {
    for _, tx := range txns {
        if err := te.executeSingleTx(db, tx); err != nil {
            return err
        }
    }
    return nil
}

func (te *TxEngineNaive) executeSingleTx(db* DB, tx *Tx) error {
    tx.Timestamp = db.ts.FetchTimestamp()
    for _, op := range tx.Ops {
        if err := te.executeOp(db, op); err != nil {
            return err
        }
    }
    return nil
}

func (te *TxEngineNaive) executeOp(db* DB, op Op) error {
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