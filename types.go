package main

import (
	"fmt"
	"strings"
	"reflect"
)

type Val interface{
	ToString() (string, error)
	CompareTo(other Val) (int, error)
}

type IntVal int
var intValMeta Val = IntVal(0)

func (this IntVal) ToString() (string, error) {
	return fmt.Sprintf("%d", this), nil
}

func (this IntVal) CompareTo(that Val) (int, error) {
	intThat, ok := that.(IntVal)
	if !ok {
		return 0, fmt.Errorf("Expected IntVal, got %+v", reflect.TypeOf(that))
	}
	return int(this - intThat), nil
}

type StringVal string
var strValMeta Val = StringVal("")

func (this StringVal) ToString() (string, error) {
	return fmt.Sprintf("%s", this), nil
}

func (this StringVal) CompareTo(that Val) (int, error) {
	thatStr, ok := that.(StringVal)
	if !ok {
		return 0, fmt.Errorf("Expected StringVal, got %+v", reflect.TypeOf(that))
	}
	return strings.Compare(string(this), string(thatStr)), nil
}

func StringColumn(name string) ColumnStat {
	return ColumnStat{
		name: name,
		meta: strValMeta,
	}
}

func IntColumn(name string) ColumnStat {
	return ColumnStat{
		name: name,
		meta: intValMeta,
	}
}
