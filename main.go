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

type FullScanExecNode struct {
	Table *Table
}

func (f FullScanExecNode) Results() (chan *Record, error) {
	out := make(chan *Record)
	go f.Table.primary.Scan(out, f.Table.txMon.newReadTID())
	return out, nil
}

func main() {
	// instantiate the table
	schema := Schema{
		cols: []ColumnStat{
			IntColumn("id"),
			StringColumn("name"),
			IntColumn("startTime"),
			IntColumn("endTime"),
			StringColumn("result"),
		},
	}
	table := Table{
		primary: &ListIndex{
			records: []RawRecord{},
			schema: &schema,
		},
		schema: schema,
	}

	// populate the table with some test data
	for i := 0; i < 100; i++ {
		table.InsertOne(
			Record{
				"id": IntVal(i),
				"name": StringVal(fmt.Sprintf("test%d", i)),
			},
		)
	}

	// TODO: query parsing...
	// TODO: query optimizing...

	// execute the following query 
	// "SELECT * FROM table WHERE id > 95"
	countQuery := CountExecNode{
		FilterExecNode{
			FullScanExecNode{
				&table,
			},
			GtExpr{
				IntIdentifierExpr("id"),
				RawIntExpr(95),
			},
		},
	}

	results, err := countQuery.Results()
	if err != nil {
		fmt.Printf("counting results: %v\n", err)
	}
	fmtResults(results)
}
