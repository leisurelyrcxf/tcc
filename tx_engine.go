package ts_promote

type TxEngine interface {
    ExecuteTxns(txs []*Tx) error
}
