package main

import "fmt"

type PrimaryIndex interface {
		Match(val Val, out chan *Record, tID uint64) error
		Scan(out chan *Record, tID uint64) error
		Insert(RawRecord) error

		/*
		ForwardScanSupported() bool
		ScanGT(val Val, out chan *Record) error
		ScanGTE(val Val, out chan *Record) error

		ReverseScanSupported() bool
		ScanLT(val Val, out chan *Record) error
		ScanLTE(val Val, out chan *Record) error
		*/
}

// dumb index with no real order... every operation is O(n)
type ListIndex struct {
	records []RawRecord
	schema *Schema
}

func (i *ListIndex) Match(val Val, out chan *Record, tID uint64) error {
	for _, rr := range i.records {
		if rr.version.existsAt(tID) && val.CompareTo(rr.content[0]) == 0 {
			r, err := rr.toRecord(i.schema)
			if err != nil {
				return err
			}
			out <- r
		}
	}
	return nil
}

func (i *ListIndex) Scan(out chan *Record, tID uint64) error {
	fmt.Printf("I am going to scan %d rows\n", len(i.records))
	for _, rr := range i.records {
		if !rr.version.existsAt(tID) {
			continue
		}
		r, err := rr.toRecord(i.schema)
		if err != nil {
			fmt.Printf("got me an err: %+v\n", err)
			return err
		}
		out <- r
	}
	close(out)
	return nil
}

func (i *ListIndex) Insert(rr RawRecord) error {
	i.records = append(i.records, rr)
	return nil
}
