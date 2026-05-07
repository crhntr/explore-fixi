package main

import "sync/atomic"

type statistics struct {
	listenerCount atomic.Int64
	listenerIDs   atomic.Int64
}

func (stat *statistics) incrementUpdateListenerCount() int64 {
	stat.listenerCount.Add(1)
	return stat.listenerIDs.Add(1)
}
func (stat *statistics) decrementUpdateListenerCount() { stat.listenerCount.Add(-1) }
