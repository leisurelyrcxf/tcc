package expr

import (
    "fmt"
    "math"
    "reflect"
)

type Context interface {
    GetVar(key string) (interface{}, bool)
    SetVar(key string, val interface{})
}

type Executor interface {
    Get(key string, ctx Context) (float64, error)
    Set(key string, val float64, ctx Context) error
}

type Expr interface {
    iExpr()
    Eval(Executor, Context) (interface{}, error)
}

var ErrParameterNumNotMatch  = fmt.Errorf("parameter number not match")
var ErrKeyTypeNotString      = fmt.Errorf("key type not string")
var ErrValueTypeNotMatch     = fmt.Errorf("value type not match")
var ErrBoolTypeNotMatch      = fmt.Errorf("bool type not match")
var ErrAssertFailed          = fmt.Errorf("assert failed")
var ErrVarNotInitialized     = fmt.Errorf("var not initialized")
var ErrAssignNonIdentifier   = fmt.Errorf("assign to non identifier")

type Type int

const (
    Boolean Type = iota
    String
    Float64
)

type ConstVal struct {
    Obj interface{}
    Typ Type
}

func NewConst(val interface{}) *ConstVal {
    typ := reflect.TypeOf(val)
    switch typ.Kind() {
    case reflect.Bool:
        return &ConstVal{
            Obj: val,
            Typ: Boolean,
        }
    case reflect.String:
        return &ConstVal{
            Obj: val,
            Typ: String,
        }
    case reflect.Float64:
        return &ConstVal{
            Obj: val,
            Typ: Float64,
        }
    default:
        panic(fmt.Sprintf("unsupported type '%s'", typ.Kind().String()))
    }
}

func (ce *ConstVal) iExpr() {}

func (ce *ConstVal) Eval(e Executor, ctx Context) (interface{}, error) {
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

type Identifier string

func NewIdentifier(i string) Identifier {
    return Identifier(i)
}

func (i Identifier) iExpr() {}

func (i Identifier) Eval(e Executor, ctx Context) (interface{}, error) {
    if val, ok := ctx.GetVar(string(i)); !ok {
        return nil, ErrVarNotInitialized
    } else {
        return val, nil
    }
}

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

type FuncName string

const (
    Get FuncName = "get"
    Set = "set"
    Assert = "assert"
    AssertEqual = "assert_equal"
    Var = "$"
)

type FuncExpr struct {
    Name FuncName
    Parameters []Expr
}

func (fe *FuncExpr) iExpr() {}

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
    case Assert:
        if len(fe.Parameters) != 1 {
            return nil, ErrParameterNumNotMatch
        }
        b, err := evalBoolean(fe.Parameters[0], e, ctx)
        if err != nil {
            return nil, err
        }
        if !b {
            return false, ErrAssertFailed
        }
        return true, nil
    case AssertEqual:
        if len(fe.Parameters) != 2 {
            return nil, ErrParameterNumNotMatch
        }
        lv, err := evalValue(fe.Parameters[0], e, ctx)
        if err != nil {
            return nil, err
        }
        rv, err := evalValue(fe.Parameters[1], e, ctx)
        if err != nil {
            return nil, err
        }
        if lv != rv {
            return false, fmt.Errorf("%s, 'got(%v) != exp(%v)'", ErrAssertFailed.Error(), lv, rv)
        }
        return true, nil
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
    EQ
    Assign
)

type BinaryExpr struct {
    Op
    Left Expr
    Right Expr
}

func (be *BinaryExpr) Eval(e Executor, ctx Context) (interface{}, error) {
    if be.Op == Assign {
        if ident, isIdentifier := be.Left.(Identifier); !isIdentifier {
            return nil, ErrAssignNonIdentifier
        } else {
            val, err := be.Right.Eval(e, ctx)
            if err != nil {
                return nil, err
            }
            ctx.SetVar(string(ident), val)
            return val, nil
        }
    }
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
    case EQ:
        return lf == rf, nil
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