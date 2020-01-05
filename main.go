package main

import (
		"fmt"
)

// Table holds all the necessary data and metadata required to
// do operations on a database table. The primary index is a
// way of storing physical tuples, and the secondary indices
// are each indexed to reference pointers to the indexes in
// primary. Since golang doesn't have strong support for generics,
// schema holds type information about records in the table.
type Table struct {
	primary PrimaryIndex
	// indices []Index
	schema  Schema
	txMon TransactionManager
}

func (t *Table) InsertOne(r Record) error {
	tID, commit := t.txMon.newWriteTID()
	defer commit()
	rr, err := r.toRawRecord(&t.schema, tID)
	if err != nil {
		return err
	}
	return t.primary.Insert(rr)
}

func (t *Table) FullScan() (chan *Record, error) {
	records := make(chan *Record)
	go t.primary.Scan(records, t.txMon.newReadTID())
	return records, nil
}

func main() {
	// instantiate the table
	jobSchema := Schema{
		cols: []ColumnStat{
			IntColumn("id"),
			StringColumn("name"),
			IntColumn("startTime"),
			IntColumn("endTime"),
			StringColumn("result"),
		},
	}
	jobTable := Table{
		primary: &ListIndex{
			records: []RawRecord{},
			schema: &jobSchema,
		},
		schema: jobSchema,
	}

	// populate the table with some test data
	for i := 0; i < 100; i++ {
		jobTable.InsertOne(
			Record{
				"id": IntVal(i),
				"name": StringVal(fmt.Sprintf("test%d", i)),
			},
		)
	}

	// TODO: query parsing...
	// TODO: query optimizing...

	// execute the following query 
	// "SELECT * FROM job WHERE id > 95"
	scanChan, _ := jobTable.FullScan()
	filterChan, _ := applyFilter(scanChan, GtExpr{
		IntIdentifierExpr("id"),
		RawIntExpr(95),
	})
	countChan, _ := countAll(filterChan)

	fmtResults(countChan)
}
