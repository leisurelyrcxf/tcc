package tcc

import (
    "fmt"
    "github.com/golang/glog"
    "tcc/expr"
    "testing"
    "time"
)

func TestTxEngineMVCCTO(t *testing.T) {
    db := NewDBWithMVCCEnabled()
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
    ),
    NewTx(
       []Op {{
           key: "a",
           typ: WriteDirect,
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
        db.values.ForEachStrict(func(_ string, vv interface{}) {
            vv.(DBVersionedValues).Clear()
        })
        i := float64(0)
        db.SetUnsafe("a", i, 0, nil); i++
        db.SetUnsafe("b", i, 0, nil); i++
        db.SetUnsafe("c", i, 0, nil); i++
        db.SetUnsafe("d", i, 0, nil); i++
        db.SetUnsafe("e", i, 0, nil); i++
        db.SetUnsafe("f", i, 0, nil); i++
        db.SetUnsafe("g", i, 0, nil); i++
        db.ts.c.Set(0)
    }

    var totalTime time.Duration
    threadNum := MaxInt(len(newTxns) / 4, 4)
    round := 10000
    glog.Infof("%d transactions in one round, %d threads, %d rounds", len(newTxns), threadNum, round)
    for i := 0; i < round; i++ {
        glog.V(3).Infof("\nRound: %d\n", i)
        duration, err := executeOneRoundMVCCTO(db, txns, initDBFunc, threadNum, false, nil)
        totalTime+= duration

        if err != nil {
            t.Errorf(err.Error())
            return
        }
        if i % 1000 == 0 {
            fmt.Printf("%d rounds finished\n", i + 1)
        }
    }
    fmt.Printf("\nCost %f seconds for %d rounds\n", float64(totalTime)/float64(time.Second), round)
}

func TestBankTransferConsistentRead(t *testing.T) {
    db := NewDBWithMVCCEnabled()

    soldA := float64(1000)
    soldB := float64(500)
    initDBFunc := func(db *DB) {
        db.values.ForEachStrict(func(_ string, vv interface{}) {
            vv.(DBVersionedValues).Clear()
        })
        db.SetUnsafe("a", soldA, 0, nil)
        db.SetUnsafe("b", soldB, 0, nil)
        db.ts.c.Set(0)
    }

    txns := []*Txn{NewTx(
        []Op {{
            key: "a",
            typ: IncrAdd,
            operatorNum: 5,
        }, {
            key: "b",
            typ: IncrAdd,
            operatorNum: -5,
        }},
    ), NewTx(
        []Op {{
            key: "a",
            typ: IncrAdd,
            operatorNum: 100,
        }, {
            key: "b",
            typ: IncrAdd,
            operatorNum: -100,
        }},
    ),
    NewTx(
        []Op {{
            key: "b",
            typ: IncrAdd,
            operatorNum: 50,
        }, {
            key: "a",
            typ: IncrAdd,
            operatorNum: -50,
        }},
    ), NewTx(
      []Op {{
          typ: Procedure,
          expr: &expr.FuncExpr{
              Name:       expr.AssertEqual,
              Parameters: []expr.Expr{
                  &expr.BinaryExpr{
                      Op:    expr.Add,
                      Left:  &expr.FuncExpr{
                          Name:       expr.Get,
                          Parameters: []expr.Expr{expr.NewConst("a")},
                      },
                      Right: &expr.FuncExpr{
                          Name:       expr.Get,
                          Parameters: []expr.Expr{expr.NewConst("b")},
                      },
                  },
                  expr.NewConst(soldA + soldB),
              },
          },
      }},
    )}

    e := 1
    newTxns := make([]*Txn, len(txns) * e)
    for i := range newTxns {
        newTxns[i] = txns[i%4].Clone()
        newTxns[i].ID = TxnIDCounter.Add(1)
    }
    txns = newTxns

    var totalTime time.Duration
    threadNum := MaxInt(len(newTxns) / 4, 4)
    round := 10000
    glog.Infof("%d transactions in one round, %d threads, %d rounds", len(newTxns), threadNum, round)
    for i := 0; i < round; i++ {
        glog.V(3).Infof("\nRound: %d\n", i)
        duration, err := executeOneRoundMVCCTO(db, txns, initDBFunc, threadNum, false, func() *TxEngineMVCCTO {
            return NewTxEngineMVCCTO(db, threadNum, false, time.Nanosecond * 500)
        })
        totalTime+= duration

        if err != nil {
            t.Errorf(err.Error())
            return
        }
        if i % 1000 == 0 {
            fmt.Printf("%d rounds finished\n", i + 1)
        }
    }
    fmt.Printf("\nCost %f seconds for %d rounds\n", float64(totalTime)/float64(time.Second), round)
}

func TestTxEngineMVCCTOProc(t *testing.T) {
    db := NewDBWithMVCCEnabled()
    txns := []*Txn{NewTx(
        []Op{{
            typ: Procedure,
            expr: &expr.IfExpr{
                Pred: &expr.BinaryExpr{
                    Op: expr.GTLE,
                    Left: &expr.BinaryExpr{
                        Op: expr.Add,
                        Left: &expr.FuncExpr{
                            Name: expr.Get,
                            Parameters: []expr.Expr{&expr.ConstVal{
                                Obj: "a",
                                Typ: expr.String,
                            }},
                        },
                        Right: &expr.FuncExpr{
                            Name: expr.Get,
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
                    Name: expr.Set,
                    Parameters: []expr.Expr{
                        &expr.ConstVal{
                            Obj: "a",
                            Typ: expr.String,
                        },
                        &expr.BinaryExpr{
                            Op: expr.Minus,
                            Left: &expr.FuncExpr{
                                Name: expr.Get,
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
        []Op{{
            typ: Procedure,
            expr: &expr.IfExpr{
                Pred: &expr.BinaryExpr{
                    Op: expr.GTLE,
                    Left: &expr.BinaryExpr{
                        Op: expr.Add,
                        Left: &expr.FuncExpr{
                            Name: expr.Get,
                            Parameters: []expr.Expr{&expr.ConstVal{
                                Obj: "a",
                                Typ: expr.String,
                            }},
                        },
                        Right: &expr.FuncExpr{
                            Name: expr.Get,
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
                    Name: expr.Set,
                    Parameters: []expr.Expr{
                        &expr.ConstVal{
                            Obj: "b",
                            Typ: expr.String,
                        },
                        &expr.BinaryExpr{
                            Op: expr.Minus,
                            Left: &expr.FuncExpr{
                                Name: expr.Get,
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

    e := 3
    newTxns := make([]*Txn, len(txns)*e)
    for i := range newTxns {
        newTxns[i] = txns[i%2].Clone()
    }
    txns = newTxns

    initDBFunc := func(db *DB) {
        db.values.ForEachStrict(func(_ string, vv interface{}) {
            vv.(DBVersionedValues).Clear()
        })
        db.SetUnsafe("a", 0, 0, nil)
        db.SetUnsafe("b", 5, 0, nil)
        db.ts.c.Set(0)
    }

    var totalTime time.Duration
    threadNum := MaxInt(len(newTxns) / 4, 4)
    round := 10000
    glog.Infof("%d transactions in one round, %d threads, %d rounds", len(newTxns), threadNum, round)
    for i := 0; i < round; i++ {
        glog.V(3).Infof("\nRound: %d\n", i)
        duration, err := executeOneRoundMVCCTO(db, txns, initDBFunc, threadNum, false, nil)
        totalTime+= duration

        if err != nil {
            t.Errorf(err.Error())
            return
        }
        if i % 1000 == 0 {
            fmt.Printf("%d rounds finished\n", i + 1)
        }
    }
    fmt.Printf("\nCost %f seconds for %d rounds\n", float64(totalTime)/float64(time.Second), round)
}

func executeOneRoundMVCCTO(db *DB, txns []*Txn, initDBFunc func(*DB), threadNum int, logRes bool,
    constructor func()*TxEngineMVCCTO) (time.Duration, error) {
    initDBFunc(db)
    for _, txn := range txns {
        txn.ResetForTestOnly()
    }

    start := time.Now()
    te := constructor()
    if constructor() == nil {
        constructor = func() *TxEngineMVCCTO {
            return NewTxEngineMVCCTO(db, threadNum, true, time.Nanosecond * 500)
        }
    }
    if err := te.ExecuteTxns(db, txns); err != nil {
        return 0, err
    }
    duration := time.Since(start)

    newTxns := make([]*Txn, len(txns))
    for i := range newTxns {
        newTxns[i] = txns[i].Tail()
        if newTxns[i].Head() != txns[i] {
           return 0, fmt.Errorf("head not correct")
        }
    }

    res := db.Snapshot()

    if err := generateDatum(db, newTxns, initDBFunc); err != nil {
        return 0, err
    }
    datum := db.Snapshot()
    if !areEqualMaps(res, datum) {
        return 0, fmt.Errorf("expect '%s', but met '%s'", SerializeMap(datum), SerializeMap(res))
    } else {
        if logRes {
            glog.Infof("res: '%s'", SerializeMap(res))
        }
    }
    return duration, nil
}


