package main

import (
	"testing"
	"reflect"
	"fmt"
)

// createDummyTable will return a new dummy table with a bunch of dummy
// values.
func createDummyTable() *Table {
	schema := Schema{
		cols: []ColumnStat{
			IntColumn("id"),
			StringColumn("name"),
			IntColumn("startTime"),
			IntColumn("endTime"),
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
		fmt.Printf("Inserting with id=%d\n", i)
		table.InsertOne(
			Record{
				"id": IntVal(i),
				"name": StringVal(fmt.Sprintf("test%d", i)),
			},
		)
	}

	return &table
}

func assertResultsMatch(t *testing.T, actual chan *Record, expected []Record) {
	for idx, expRecord := range expected {
		actRecord, ok := <-actual

		if actRecord == nil || !ok {
			t.Fatalf("Ran out of results at index %d\n", idx)
		}
		if !reflect.DeepEqual(*actRecord, expRecord) {
			t.Errorf("At index %d:\n\tExpected\n\t\t%+v\n\tGot\n\t\t%+v\n", idx, expRecord, actRecord)
		}
	}

	_, ok := <-actual
	if ok {
		t.Errorf("Expected results channel to be closed. It is open.\n")
	}
}

func TestInsertCount(t *testing.T) {
	table := createDummyTable()

	// SELECT COUNT(*) FROM table
	scanChan, _ := table.FullScan()
	countChan, _ := countAll(scanChan)

	assertResultsMatch(t,
			countChan,
			[]Record{Record{"COUNT": IntVal(100)}})
}

func TestInsertFilterCount(t *testing.T) {
	table := createDummyTable()

	// SELECT COUNT(*) FROM table WHERE id > 95
	scanChan, _ := table.FullScan()
	filterChan, _ := applyFilter(scanChan, GtExpr{
		IntIdentifierExpr("id"),
		RawIntExpr(95),
	})
	countChan, _ := countAll(filterChan)

	assertResultsMatch(t,
			countChan,
			[]Record{Record{"COUNT": IntVal(4)}})
}
