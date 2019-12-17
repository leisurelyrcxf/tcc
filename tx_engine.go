package tcc

type TxEngine interface {
    ExecuteTxns(db* DB, txns []*Txn) error
}
