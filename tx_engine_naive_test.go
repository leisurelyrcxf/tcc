package tcc

import (
    "fmt"
    "tcc/expr"
    "testing"
)

func TestTxEngineNaive_ExecuteTxns(t *testing.T) {
    db := NewDB()
    db.SetUnsafe("a", 1.0, 0, nil)
    db.SetUnsafe("b", 2.0, 0, nil)

    txns := []*Txn{NewTx(
        []Op {{
            key: "a",
            typ: IncrAdd,
            operatorNum: 1,
        }, {
            key: "b",
            typ: IncrAdd,
            operatorNum: 2,
        }},
    ), NewTx(
        []Op {{
            key: "a",
            typ: IncrAdd,
            operatorNum: 1,
        }, {
            key: "b",
            typ: IncrAdd,
            operatorNum: 2,
        }},
    ), NewTx(
        []Op {{
            key: "a",
            typ: IncrAdd,
            operatorNum: 1,
        }, {
            key: "b",
            typ: IncrAdd,
            operatorNum: 2,
        }},
    )}

    var te TxEngineNaive
    err := te.ExecuteTxns(db, txns)
    if err != nil {
        t.Errorf(err.Error())
        return
    }
    if val, _ := db.Get("a"); val != 4 {
        t.Errorf("value for a failed")
        return
    }

    if val, _ := db.Get("b"); val != 8 {
        t.Errorf("value for a failed")
        return
    }
    for _, txn := range txns {
        fmt.Println(txn.GetTimestamp())
    }
}

func TestTxEngineNaive_Expr(t *testing.T) {
    db := NewDB()
    db.SetUnsafe("a", 0, 0, nil)
    db.SetUnsafe("b", 5.0, 0, nil)

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
                            Parameters: []expr.Expr{&expr.ConstExpr{
                                Obj: "a",
                                Typ: expr.String,
                            }},
                        },
                        Right: &expr.FuncExpr{
                            Name:       expr.Get,
                            Parameters: []expr.Expr{&expr.ConstExpr{
                                Obj: "b",
                                Typ: expr.String,
                            }},
                        },
                    },
                    Right: &expr.ConstExpr{
                        Obj: 5,
                        Typ: expr.Float64,
                    },
                },
                Then: &expr.FuncExpr{
                    Name:       expr.Set,
                    Parameters: []expr.Expr{
                        &expr.ConstExpr{
                            Obj: "a",
                            Typ: expr.String,
                        },
                        &expr.BinaryExpr{
                            Op:    expr.Minus,
                            Left:  &expr.FuncExpr{
                                Name:       expr.Get,
                                Parameters: []expr.Expr{&expr.ConstExpr{
                                    Obj: "a",
                                    Typ: expr.String,
                                }},
                            },
                            Right: &expr.ConstExpr{
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
                            Parameters: []expr.Expr{&expr.ConstExpr{
                                Obj: "a",
                                Typ: expr.String,
                            }},
                        },
                        Right: &expr.FuncExpr{
                            Name:       expr.Get,
                            Parameters: []expr.Expr{&expr.ConstExpr{
                                Obj: "b",
                                Typ: expr.String,
                            }},
                        },
                    },
                    Right: &expr.ConstExpr{
                        Obj: 5,
                        Typ: expr.Float64,
                    },
                },
                Then: &expr.FuncExpr{
                    Name:       expr.Set,
                    Parameters: []expr.Expr{
                        &expr.ConstExpr{
                            Obj: "b",
                            Typ: expr.String,
                        },
                        &expr.BinaryExpr{
                            Op:    expr.Minus,
                            Left:  &expr.FuncExpr{
                                Name:       expr.Get,
                                Parameters: []expr.Expr{&expr.ConstExpr{
                                    Obj: "b",
                                    Typ: expr.String,
                                }},
                            },
                            Right: &expr.ConstExpr{
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

    e := 10
    newTxns := make([]*Txn, len(txns) * e)
    for i := range newTxns {
        newTxns[i] = txns[i%2].Clone()
    }
    txns = newTxns

    te := NewTxEngineNaive(db)
    err := te.ExecuteTxns(db, txns)
    if err != nil {
        t.Errorf(err.Error())
        return
    }
    if val, _ := db.Get("a"); val != -5 {
        t.Errorf("value for a failed")
        return
    }

    if val, _ := db.Get("b"); val != 5 {
        t.Errorf("value for b failed")
        return
    }
    for _, txn := range txns {
        fmt.Println(txn.GetTimestamp())
    }
}
