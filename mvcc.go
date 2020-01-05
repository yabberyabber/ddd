package main

import (
	"sync"
	"sync/atomic"
)

// VersionRange represents the range of transaction versions for which this
// record exists. Min must be a nonzero positive integer. Max must be a
// non-negative integer. If max is 0, then this record still exists in the
// latest version.
type VersionRange struct {
	min uint64
	max uint64
}

func (v *VersionRange) existsAt(transactionID uint64) bool {
	if transactionID <= v.min {
		return false
	}
	if v.max == 0 {
		return true
	}
	return transactionID <= v.max
}

// TransactionManager is responsible for generating and tracking
// transaction IDs
type TransactionManager struct {
	// for now we have a global write lock
	writeLock sync.Mutex
	lastCompleteTID atomic.Value
}

// newReadTID gets a transaction ID to run a read query with. By default
// this is the TID of the latest completed write.
func (t *TransactionManager) newReadTID() uint64 {
	tID := t.lastCompleteTID.Load()
	if tID == nil {
		return 1
	}
	return tID.(uint64)
}

// newWriteTID creates a new transaction ID to track the results of
// this write query. For now we will only allow one write operation
// at a time, but any read query that want to start can do so using
// the transaction ID of everything committed before this write query.
func (t *TransactionManager) newWriteTID() (uint64, func()) {
	t.writeLock.Lock()
	tID := t.newReadTID() + 1
	commit := func () {
		t.lastCompleteTID.Store(tID)
		t.writeLock.Unlock()
	}
	return tID, commit
}
