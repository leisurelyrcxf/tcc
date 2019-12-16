package ts_promote

type TxEngine interface {
    ExecuteTxns(db* DB, txns []*Txn) error
}
