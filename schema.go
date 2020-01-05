package main

import(
		"fmt"
)

type Schema struct {
	cols []ColumnStat
}

// RawRecord is how records are stored in the primary index. Elements
// of the RawRecord corresponds to elements in the table's Schema slice.
// i.e., table.schema[0] describes the type of RawRecord[0]. This is a
// more space-efficient representation than Record because each Val doesn't
// need to point to its type information. Also, when we implement
// persistance, RawRecords are what will be written to disk.
type RawRecord []interface{}

// Record is how records are represented during computation. It is formed
// as a map for easy handling and all the values are wrapped in a Val so
// that we don't have to keep looking up type information for each
// value.
type Record map[string]Val

func (r Record) String() string {
	result := ""
	for key, val := range r {
		result += fmt.Sprintf("%12s: %#v\n", key, val.raw)
	}
	return result
}

func (r RawRecord) toRecord(s *Schema) (*Record, error) {
	if len(r) != len(s.cols) {
		return nil, fmt.Errorf("invalid record - record width %d != schema width %d (record is %+v, r[0] is %+v)",
				len(r), len(s.cols), r, r[0])
	}

	res := Record{}
	for i, col := range s.cols {
		res[col.name] = Val{
			raw: r[i],
			meta: col.meta,
		}
	}

	return &res, nil
}

func (r Record) toRawRecord(s *Schema) (RawRecord, error) {
	rr := make([]interface{}, len(s.cols))
	for idx, col := range s.cols {
		rr[idx] = r[col.name].raw
	}
	return rr, nil
}

type ColumnStat struct {
	name string
	meta *TypeMeta
	defaultGen DefaultGen
}

// TypeMeta wraps a type. If we want to support a type in our expressions,
// it needs to have a TypeMeta struct to explain how to print and compare it.
type TypeMeta struct {
	toString func(interface{}) string
	compare func(interface{}, interface{}) int
}

// Val is a runtime-typed value in a record. It may have been cleaner to define
// Val as an interface which defines toString and compare, but then we wouldn't
// be able to use the TypeMeta types to decribe table schemas.
type Val struct {
	raw interface{}
	meta *TypeMeta
}

func (v Val) CompareTo(rawVal interface{}) int {
	return v.meta.compare(v.raw, rawVal)
}

