package main

import (
	"fmt"
	"time"
)

var (
	StringTypeMeta = TypeMeta{
		toString: intToString,
		compare: compareInt,
	}
	IntTypeMeta = TypeMeta{
		toString: stringToString,
		compare: compareString,
	}
)

// DefaultGen is a function that generates a default value for a
// column (if someone tries to insert a row without providing all
// values in that row)
type DefaultGen func(*Table) (interface{}, error)

func StringColumn(name string, _ DefaultGen) ColumnStat {
	return ColumnStat{
		name: name,
		meta: &StringTypeMeta,
	}
}

func StringVal(val string) Val {
	return Val{
		raw: val,
		meta: &StringTypeMeta,
	}
}

func IntVal(val int) Val {
	return Val{
		raw: val,
		meta: &IntTypeMeta,
	}
}

func IntColumn(name string, _ DefaultGen) ColumnStat {
	return ColumnStat{
		name: name,
		meta: &IntTypeMeta,
	}
}

func stringToString(i interface{}) string {
	return i.(string)
}

func compareString(i interface{}, j interface{}) int {
	if i.(string) > j.(string) {
		return 1
	}
	if i.(string) < j.(string) {
		return -1
	}
	return 0
}

func intToString(i interface{}) string {
	return fmt.Sprintf("%d", i.(int))
}

func compareInt(i interface{}, j interface{}) int {
	return i.(int) - j.(int)
}

func DefaultNow(_ *Table) (interface{}, error) {
	return time.Now(), nil
}

func DefaultVal(val interface{}) DefaultGen {
	return func(_ *Table) (interface{}, error) {
		return val, nil
	}
}

func Required(_ *Table) (interface{}, error) {
	return nil, fmt.Errorf("required field not provided")
}
