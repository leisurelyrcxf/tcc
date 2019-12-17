package tcc

import "tcc/sync2"

type TimeServer struct {
    c sync2.AtomicInt64
}

func NewTimeServer() *TimeServer {
    return &TimeServer{c: sync2.NewAtomicInt64(0)}
}

func (ts *TimeServer) FetchTimestamp() int64 {
    return ts.c.Add(1)
}