package tcc

import (
    "fmt"
    "github.com/golang/glog"
    "sort"
    "sync"
    "tcc/expr"
    "testing"
    "time"
)

func TestTxEngineTimestampOrdering(t *testing.T) {
    db := NewDB()
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

    e := 1
    newTxns := make([]*Txn, len(txns) * e)
    for i := range newTxns {
        newTxns[i] = txns[i%4].Clone()
        newTxns[i].ID = TxnIDCounter.Add(1)
    }
    txns = nil

    initDBFunc := func (db *DB) {
        db.SetUnsafe("a", 0, 0, nil)
        db.SetUnsafe("b", 1, 0, nil)
        db.ts.c.Set(0)
    }

    var totalTime time.Duration
    round := 10000
    threadNum := MaxInt(len(newTxns) / 4, 4)
    glog.Infof("%d transactions in one round, %d threads, %d rounds", len(newTxns), threadNum, round)
    for i := 0; i < round; i++ {
        glog.V(10).Infof("\nRound: %d\n", i)
        duration, err := executeOneRound(db, newTxns, initDBFunc)
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

func TestTxEngineTimestampOrderingProcWriteSkew(t *testing.T) {
    db := NewDB()
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

    e := 3
    newTxns := make([]*Txn, len(txns) * e)
    for i := range newTxns {
        newTxns[i] = txns[i%2].Clone()
        newTxns[i].Keys = []string{"a", "b"}
    }
    txns = newTxns

    initDBFunc := func (db *DB) {
        db.SetUnsafe("a", 0, 0, nil)
        db.SetUnsafe("b", 5, 0, nil)
        db.ts.c.Set(0)
    }

    var totalTime time.Duration
    round := 10000
    for i := 0; i < round; i++ {
        glog.V(10).Infof("\nRound: %d\n", i)
        duration, err := executeOneRound(db, txns, initDBFunc)
        totalTime+= duration

        if err != nil {
            t.Errorf(err.Error())
            return
        }
        if i % 1000 == 0 {
            fmt.Printf("%d rounds finished\n", i)
        }
    }
    fmt.Printf("\nCost %f seconds for %d rounds\n", float64(totalTime)/float64(time.Second), round)
}

func executeOneRound(db *DB, txns []*Txn, initDBFunc func(*DB)) (time.Duration, error) {
    initDBFunc(db)
    for _, txn := range txns {
        txn.ResetForTestOnly()
    }

    start := time.Now()
    newTxns := make([]*Txn, 0, len(txns))
    var newTxnsMutex sync.Mutex
    te := NewTxEngineTO(db,4, db.lm, time.Microsecond * 3)
    te.AddPostCommitListener(func(txn *Txn) {
        newTxnsMutex.Lock()
        newTxns = append(newTxns, txn)
        newTxnsMutex.Unlock()
    })
    if err := te.ExecuteTxns(db, txns); err != nil {
        return 0, err
    }
    duration := time.Since(start)
    if len(newTxns) != len(txns) {
        return 0, fmt.Errorf("not the same, expect %d, but met %d", len(txns), len(newTxns))
    }
    res := db.Snapshot()

    if err := generateDatum(db, newTxns, initDBFunc); err != nil {
        return 0, err
    }
    datum := db.Snapshot()
    if !areEqualMaps(res, datum) {
        return 0, fmt.Errorf("expect '%s', but met '%s'", SerializeMap(datum), SerializeMap(res))
    }
    return duration, nil
}

func generateDatum(db *DB, txns []*Txn, initDBFunc func(*DB)) error {
    sort.Sort(TxnSliceSortByTimestamp(txns))
    defer sort.Sort(TxnSliceSortByTxID(txns))
    for _, txn := range txns {
        txn.ResetForTestOnly()
    }
    initDBFunc(db)
    ten := NewTxEngineNaive(db)
    if err := ten.ExecuteTxns(db, txns); err != nil {
        return fmt.Errorf("execute failed for oneOrderTxns, detail: '%s'", err.Error())
    }
    return nil
}


