package ts_promote

import "fmt"

type TxEngineTO struct {

}

func (te *TxEngineTO) ExecuteTxns(db* DB, txs []Tx) error {
    for _, tx := range txs {
        if err := te.executeSingleTx(db, tx); err != nil {
            return err
        }
    }
    return nil
}

func (te *TxEngineTO) executeSingleTx(db* DB, tx Tx) error {
    for _, op := range tx.Ops {
        if err := te.executeOp(db, op); err != nil {
            return err
        }
    }
    return nil
}

func (te *TxEngineTO) executeOp(db* DB, op Op) error {
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