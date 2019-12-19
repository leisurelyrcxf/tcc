package expr

import (
    "fmt"
    "math"
    "reflect"
)

type Context map[string]float64

type Executor interface {
    Get(key string, ctx Context) (float64, error)
    Set(key string, val float64, ctx Context) error
}

type Expr interface {
    iExpr()
    Eval(Executor, Context) (interface{}, error)
}

type Type int

const (
    Boolean Type = iota
    String
    Float64
)

type ConstExpr struct {
    Obj interface{}
    Typ Type
}

func (ce *ConstExpr) iExpr() {}

func (ce *ConstExpr) Eval(e Executor, ctx Context) (interface{}, error) {
    switch ce.Typ {
    case Boolean:
        return ce.Obj.(bool), nil
    case Float64:
        if t, ok := ce.Obj.(float64); ok {
            return t, nil
        }
        var f float64
        return reflect.ValueOf(ce.Obj).Convert(reflect.TypeOf(f)).Interface().(float64), nil
    case String:
        return ce.Obj.(string), nil
    default:
        panic("not implemented yet")
    }
}

const (
    Get = "get"
    Set = "set"
)

type FuncExpr struct {
    Name string
    Parameters []Expr
}

func (fe *FuncExpr) iExpr() {}

var ErrParameterNumNotMatch = fmt.Errorf("parameter number not match")
var ErrKeyTypeNotString = fmt.Errorf("key type not string")
var ErrValueTypeNotMatch = fmt.Errorf("value type not match")
var ErrBoolTypeNotMatch = fmt.Errorf("bool type not match")

func evalKey(expr Expr, e Executor, ctx Context) (string, error) {
    keyObj, err := expr.Eval(e, ctx)
    if err != nil {
        return "", err
    }
    key, ok := keyObj.(string)
    if !ok {
        return "", ErrKeyTypeNotString
    }
    return key, nil
}

func evalValue(expr Expr, e Executor, ctx Context) (float64, error) {
    valueObj, err := expr.Eval(e, ctx)
    if err != nil {
        return math.NaN(), err
    }
    value, ok := valueObj.(float64)
    if !ok {
        return math.NaN(), ErrValueTypeNotMatch
    }
    return value, nil
}

func evalBoolean(expr Expr, e Executor, ctx Context) (bool, error) {
    bObj, err := expr.Eval(e, ctx)
    if err != nil {
        return false, err
    }
    b, ok := bObj.(bool)
    if !ok {
        return false, ErrBoolTypeNotMatch
    }
    return b, nil
}

func (fe *FuncExpr) Eval(e Executor, ctx Context) (interface{}, error) {
    switch fe.Name {
    case Get:
        if len(fe.Parameters) != 1 {
            return nil, ErrParameterNumNotMatch
        }
        key, err := evalKey(fe.Parameters[0], e, ctx)
        if err != nil {
            return nil, err
        }
        return e.Get(key, ctx)
    case Set:
        if len(fe.Parameters) != 2 {
            return nil, ErrParameterNumNotMatch
        }
        key, err := evalKey(fe.Parameters[0], e, ctx)
        if err != nil {
            return nil, err
        }
        value, err := evalValue(fe.Parameters[1], e, ctx)
        if err != nil {
            return nil, err
        }
        return nil, e.Set(key, value, ctx)
    default:
        panic("not implemented yet")
    }
}

type Op int

const (
    Add Op = iota
    Minus
    GT
    GTLE
)

type BinaryExpr struct {
    Op
    Left Expr
    Right Expr
}

func (be *BinaryExpr) Eval(e Executor, ctx Context) (interface{}, error) {
    lf, err := evalValue(be.Left, e, ctx)
    if err != nil {
        return nil, err
    }
    rf, err := evalValue(be.Right, e, ctx)
    if err != nil {
        return nil, err
    }
    switch be.Op {
    case Add:
        return lf + rf, nil
    case Minus:
        return lf - rf, nil
    case GT:
        return lf > rf, nil
    case GTLE:
        return lf >= rf, nil
    default:
        panic("not implemented yet")
    }
}

func (be *BinaryExpr) iExpr() {}

type IfExpr struct {
    Pred Expr
    Then Expr
    Else Expr
}

func (ie *IfExpr) iExpr() {}

func (ie *IfExpr) Eval(e Executor, ctx Context) (interface{}, error) {
    b, err := evalBoolean(ie.Pred, e, ctx)
    if err != nil {
        return nil, err
    }
    if b {
        if ie.Then == nil {
            return nil, nil
        }
        return ie.Then.Eval(e, ctx)
    } else {
        if ie.Else == nil {
            return nil, nil
        }
        return ie.Else.Eval(e, ctx)
    }
}