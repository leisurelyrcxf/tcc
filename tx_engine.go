package tcc

type TxEngine interface {
    AddPostCommitListener(cb func(*Txn))
    ExecuteTxns(db* DB, txns []*Txn) error
}
