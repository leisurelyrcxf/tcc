package tcc

import (
    "github.com/golang/glog"
    "tcc/expr"
    "testing"
)

func TestNewTxEngineC2PL(t *testing.T) {
    txns := []*Txn{NewTx(
        []Op {{
            key: "a",
            typ: IncrAdd,
            operatorNum: 1,
        }, {
            key: "b",
            typ: IncrMultiply,
            operatorNum: 2,
        }},
    ), NewTx(
        []Op {{
            key: "a",
            typ: IncrMultiply,
            operatorNum: 2,
        }, {
            key: "b",
            typ: IncrAdd,
            operatorNum: 1,
        }},
    ),
    NewTx(
       []Op {{
           key: "b",
           typ: IncrMultiply,
           operatorNum: 20,
       }, {
           key: "a",
           typ: IncrAdd,
           operatorNum: 10,
       }},
    ), NewTx(
       []Op {{
           key:         "a",
           typ:         WriteDirect,
           operatorNum: 100,
       }},
    ),
    }

    e := 10
    newTxns := make([]*Txn, len(txns) * e)
    for i := range newTxns {
        newTxns[i] = txns[i%4].Clone()
        newTxns[i].ID = TxnIDCounter.Add(1)
    }
    txns = newTxns

    initDBFunc := func (db *DB) {
        db.SetUnsafe("a", 0, 0, nil)
        db.SetUnsafe("b", 1, 0, nil)
        db.ts.c.Set(0)
    }

    threadNum := MaxInt(len(newTxns) / 4, 4)
    round := 10000
    glog.Infof("%d transactions in one round, %d threads, %d rounds", len(newTxns), threadNum, round)

    db := NewDB()
    TxTest(t, db, newTxns, initDBFunc, func() TxEngine {
        return NewTxEngineC2PL(db, threadNum)
    }, round, len(newTxns) <= 6, len(newTxns) <= 4)
}

func Test2PCL_Procedure(t *testing.T) {
    txns := []*Txn{NewTx(
        []Op {{
            typ: Procedure,
            expr: &expr.IfExpr{
                Pred: &expr.BinaryExpr{
                    Op:    expr.GTLE,
                    Left:  &expr.BinaryExpr{
                        Op:    expr.Add,
                        Left:  &expr.FuncExpr{
                            Name:       expr.Get,
                            Parameters: []expr.Expr{&expr.ConstVal{
                                Obj: "a",
                                Typ: expr.String,
                            }},
                        },
                        Right: &expr.FuncExpr{
                            Name:       expr.Get,
                            Parameters: []expr.Expr{&expr.ConstVal{
                                Obj: "b",
                                Typ: expr.String,
                            }},
                        },
                    },
                    Right: &expr.ConstVal{
                        Obj: 5,
                        Typ: expr.Float64,
                    },
                },
                Then: &expr.FuncExpr{
                    Name:       expr.Set,
                    Parameters: []expr.Expr{
                        &expr.ConstVal{
                            Obj: "a",
                            Typ: expr.String,
                        },
                        &expr.BinaryExpr{
                            Op:    expr.Minus,
                            Left:  &expr.FuncExpr{
                                Name:       expr.Get,
                                Parameters: []expr.Expr{&expr.ConstVal{
                                    Obj: "a",
                                    Typ: expr.String,
                                }},
                            },
                            Right: &expr.ConstVal{
                                Obj: 5,
                                Typ: expr.Float64,
                            },
                        },
                    },
                },
                Else: nil,
            },
        }},
    ), NewTx(
        []Op {{
            typ: Procedure,
            expr: &expr.IfExpr{
                Pred: &expr.BinaryExpr{
                    Op:    expr.GTLE,
                    Left:  &expr.BinaryExpr{
                        Op:    expr.Add,
                        Left:  &expr.FuncExpr{
                            Name:       expr.Get,
                            Parameters: []expr.Expr{&expr.ConstVal{
                                Obj: "a",
                                Typ: expr.String,
                            }},
                        },
                        Right: &expr.FuncExpr{
                            Name:       expr.Get,
                            Parameters: []expr.Expr{&expr.ConstVal{
                                Obj: "b",
                                Typ: expr.String,
                            }},
                        },
                    },
                    Right: &expr.ConstVal{
                        Obj: 5,
                        Typ: expr.Float64,
                    },
                },
                Then: &expr.FuncExpr{
                    Name:       expr.Set,
                    Parameters: []expr.Expr{
                        &expr.ConstVal{
                            Obj: "b",
                            Typ: expr.String,
                        },
                        &expr.BinaryExpr{
                            Op:    expr.Minus,
                            Left:  &expr.FuncExpr{
                                Name:       expr.Get,
                                Parameters: []expr.Expr{&expr.ConstVal{
                                    Obj: "b",
                                    Typ: expr.String,
                                }},
                            },
                            Right: &expr.ConstVal{
                                Obj: 5,
                                Typ: expr.Float64,
                            },
                        },
                    },
                },
                Else: nil,
            },
        }},
    )}

    // Will fail due to write-skew
    // txns[0].Keys = []string{"a"}; txns[1].Keys = []string{"b"}
    txns[0].Keys = []string{"a", "b"}; txns[1].Keys = []string{"b", "a"}
    e := 2
    newTxns := make([]*Txn, len(txns) * e)
    for i := range newTxns {
        newTxns[i] = txns[i%2].Clone()
    }
    txns = newTxns

    initDBFunc := func (db *DB) {
        db.SetUnsafe("a", 0, 0, nil)
        db.SetUnsafe("b", 5, 0, nil)
        db.ts.c.Set(0)
    }

    db := NewDB()
    TxTest(t, db, txns, initDBFunc, func() TxEngine {
        return NewTxEngineC2PL(db, 4)
    }, 10000, len(newTxns) <= 6, len(newTxns) <= 4)
}


