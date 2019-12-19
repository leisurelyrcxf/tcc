package expr

import (
    "fmt"
    "math"
    "reflect"
)

type Context map[string]float64

type KV interface {
    Get(key string) (float64, error)
    Set(key string, val float64) error
    GetContext() Context
}

type Expr interface {
    iExpr()
    Eval(KV) (interface{}, error)
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

func (ce *ConstExpr) Eval(e KV) (interface{}, error) {
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

func evalKey(expr Expr, e KV) (string, error) {
    keyObj, err := expr.Eval(e)
    if err != nil {
        return "", err
    }
    key, ok := keyObj.(string)
    if !ok {
        return "", ErrKeyTypeNotString
    }
    return key, nil
}

func evalValue(expr Expr, e KV) (float64, error) {
    valueObj, err := expr.Eval(e)
    if err != nil {
        return math.NaN(), err
    }
    value, ok := valueObj.(float64)
    if !ok {
        return math.NaN(), ErrValueTypeNotMatch
    }
    return value, nil
}

func evalBoolean(expr Expr, e KV) (bool, error) {
    bObj, err := expr.Eval(e)
    if err != nil {
        return false, err
    }
    b, ok := bObj.(bool)
    if !ok {
        return false, ErrBoolTypeNotMatch
    }
    return b, nil
}

func (fe *FuncExpr) Eval(e KV) (interface{}, error) {
    ctx := e.GetContext()
    switch fe.Name {
    case Get:
        if len(fe.Parameters) != 1 {
            return nil, ErrParameterNumNotMatch
        }
        key, err := evalKey(fe.Parameters[0], e)
        if err != nil {
            return nil, err
        }
        if val, ok := ctx[key]; ok {
            return val, nil
        }
        val, err := e.Get(key)
        if err == nil {
            ctx[key] = val
        }
        return val, err
    case Set:
        if len(fe.Parameters) != 2 {
            return nil, ErrParameterNumNotMatch
        }
        key, err := evalKey(fe.Parameters[0], e)
        if err != nil {
            return nil, err
        }
        value, err := evalValue(fe.Parameters[1], e)
        if err != nil {
            return nil, err
        }
        return nil, e.Set(key, value)
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

func (be *BinaryExpr) Eval(e KV) (interface{}, error) {
    lf, err := evalValue(be.Left, e)
    if err != nil {
        return nil, err
    }
    rf, err := evalValue(be.Right, e)
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

func (ie *IfExpr) Eval(e KV) (interface{}, error) {
    b, err := evalBoolean(ie.Pred, e)
    if err != nil {
        return nil, err
    }
    if b {
        if ie.Then == nil {
            return nil, nil
        }
        return ie.Then.Eval(e)
    } else {
        if ie.Else == nil {
            return nil, nil
        }
        return ie.Else.Eval(e)
    }
}