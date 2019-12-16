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

func GetAllPossibleSerializableResult(db *DB, txns []*Tx, initDBFunc func(db *DB), printInfo bool) ([]map[string]float64, error) {
    initDBFunc(db)

    for _, txn := range txns {
        txn.Timestamp = db.ts.FetchTimestamp()
    }

    var allPossibleExecuteResults []map[string]float64
    var ten TxEngineNaive
    ch := Permutate(txns)
    for oneOrderTxns := range ch {
        if oneOrderTxns == nil {
            continue
        }

        initDBFunc(db)
        if err := ten.ExecuteTxns(db, oneOrderTxns); err != nil {
            err = fmt.Errorf("execute failed for oneOrderTxns, detail: '%s'", err.Error())
            if printInfo {
                fmt.Println(err.Error())
            }
            return nil, err
        }

        oneResult := make(map[string]float64)
        oneResult["a"], _ = db.get("a")
        oneResult["b"], _ = db.get("b")
        if printInfo {
            fmt.Printf("One possible serialize order: %s. Result: %s\n", SerializeTxns(oneOrderTxns), SerializeMap(oneResult))
        }
        allPossibleExecuteResults = append(allPossibleExecuteResults, oneResult)
    }

    return allPossibleExecuteResults, nil
}