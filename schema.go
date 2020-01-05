package main

import(
		"fmt"
)

type Schema struct {
	cols []ColumnStat
}

// RawRecord is how records are stored in the primary index. Every RawRecord
// must be as long as the schema for the table - they are complete tuples.
type RawRecord struct {
	content []Val
	version VersionRange
}

// Record is how records are represented during computation. It is formed
// as a map for easy handling and all the values are wrapped in a Val so
// that we don't have to keep looking up type information for each
// value. In many cases, the schema for a Record may not associate with
// the schema for any existing table (for an example of this, see
// aggregators.go)
type Record map[string]Val

func (r Record) String() string {
	result := ""
	for key, val := range r {
		result += fmt.Sprintf("%12s: %#v\n", key, val)
	}
	return result
}

func (r RawRecord) toRecord(s *Schema) (*Record, error) {
	if len(r.content) != len(s.cols) {
		return nil, fmt.Errorf("invalid record - record width %d != schema width %d (record is %+v)",
				len(r.content), len(s.cols), r)
	}

	res := Record{}
	for i, col := range s.cols {
		res[col.name] = r.content[i]
	}

	return &res, nil
}

func (r Record) toRawRecord(s *Schema, tID uint64) (RawRecord, error) {
	rrc := make([]Val, len(s.cols))
	for idx, col := range s.cols {
		rrc[idx] = r[col.name]
	}
	return RawRecord{
		content: rrc,
		version: VersionRange{tID, 0},
	}, nil
}

type ColumnStat struct {
	name string
	// meta is of type Val, but really we just use meta.(type) to track type information
	meta Val
}
