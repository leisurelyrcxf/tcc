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
    round := 10000
    for i := 0; i < round; i++ {
        glog.V(10).Infof("\nRound: %d\n", i)
        duration, err := executeOneRoundMVCCTO(db, txns, initDBFunc, false)
        totalTime+= duration

        if err != nil {
            t.Errorf(err.Error())
            return
        }
        if i % 100 == 0 {
            fmt.Printf("%d rounds finished\n", i)
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
                            Parameters: []expr.Expr{&expr.ConstExpr{
                                Obj: "a",
                                Typ: expr.String,
                            }},
                        },
                        Right: &expr.FuncExpr{
                            Name: expr.Get,
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
                    Name: expr.Set,
                    Parameters: []expr.Expr{
                        &expr.ConstExpr{
                            Obj: "a",
                            Typ: expr.String,
                        },
                        &expr.BinaryExpr{
                            Op: expr.Minus,
                            Left: &expr.FuncExpr{
                                Name: expr.Get,
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
        []Op{{
            typ: Procedure,
            expr: &expr.IfExpr{
                Pred: &expr.BinaryExpr{
                    Op: expr.GTLE,
                    Left: &expr.BinaryExpr{
                        Op: expr.Add,
                        Left: &expr.FuncExpr{
                            Name: expr.Get,
                            Parameters: []expr.Expr{&expr.ConstExpr{
                                Obj: "a",
                                Typ: expr.String,
                            }},
                        },
                        Right: &expr.FuncExpr{
                            Name: expr.Get,
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
                    Name: expr.Set,
                    Parameters: []expr.Expr{
                        &expr.ConstExpr{
                            Obj: "b",
                            Typ: expr.String,
                        },
                        &expr.BinaryExpr{
                            Op: expr.Minus,
                            Left: &expr.FuncExpr{
                                Name: expr.Get,
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
    round := 10000
    for i := 0; i < round; i++ {
        glog.V(10).Infof("\nRound: %d\n", i)
        duration, err := executeOneRoundMVCCTO(db, txns, initDBFunc, false)
        totalTime+= duration

        if err != nil {
            t.Errorf(err.Error())
            return
        }
        if i % 100 == 0 {
            fmt.Printf("%d rounds finished\n", i)
        }
    }
    fmt.Printf("\nCost %f seconds for %d rounds\n", float64(totalTime)/float64(time.Second), round)
}

func executeOneRoundMVCCTO(db *DB, txns []*Txn, initDBFunc func(*DB), logRes bool) (time.Duration, error) {
    initDBFunc(db)
    for _, txn := range txns {
        txn.ResetForTestOnly()
    }

    //var newTxnsMutex sync.Mutex
    //var dtTxns []*Txn
    //te.AddPostCommitListener(func(txn *Txn) {
    //    newTxnsMutex.Lock()
    //    dtTxns = append(dtTxns, txn)
    //    newTxnsMutex.Unlock()
    //})

    start := time.Now()
    te := NewTxEngineMVCCTO(db, 4, db.lm, true)
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

    //strMapper := func(obj interface{})string {
    //    return fmt.Sprintf("{%s}", obj.(*Txn).String())
    //}
    //sort.Sort(TxnSliceSortByTxID(dtTxns))
    //newStr := Array2String(newTxns, strMapper)
    //dtStr := Array2String(dtTxns, strMapper)
    //if newStr != dtStr {
    //    return 0, fmt.Errorf("array not equal, exp \n%s, but met %s\n", dtStr, newStr)
    //}

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


