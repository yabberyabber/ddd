package main

import (
	"fmt"
)

// applyFilter takes as arguments a Record chan and a boolean expression,
// checks every record in the input channel against the boolean expression,
// and sends matching records through the output chan.
func applyFilter(in chan *Record, filter BoolExpr) (chan *Record, error) {
	out := make(chan *Record)
	go func() {
		for r := range in {
			ok, err := filter.eval(r)
			if err != nil {
				panic(err)
			}
			if ok {
				out <- r
			}
		}
		close(out)
	}()
	return out, nil
}

// BoolExpr is any expression that results in a boolean value.
// Examples (x > y), (x = y), true, false, (x and y), (x or y), etc
type BoolExpr interface {
	eval(r *Record) (bool, error)
}
// StringExpr is any expression that results in a string value.
// Examples "oops", CONCAT(x, y), etc
type StringExpr interface {
	eval(r *Record) (string, error)
}
// IntExpr is any expression that results in an integer value.
// Examples 5, x + y, x - y, -x, etc
type IntExpr interface {
	eval(r *Record) (int, error)
}

// GtExpr implements BoolExpr... evaluates to true iff lhe > rhe
type GtExpr struct {
	lhe IntExpr
	rhe IntExpr
}

func (f GtExpr) eval(r *Record) (bool, error) {
	lhi, err := f.lhe.eval(r)
	if err != nil {
		return false, err
	}
	rhi, err := f.rhe.eval(r)
	if err != nil {
		return false, err
	}
	return lhi > rhi, nil
}

// RawIntExpr implements IntExpr... evaluates to whatever number is given
// (this is how we represent integer literals in our AST)
type RawIntExpr int

func (f RawIntExpr) eval(r *Record) (int, error) {
	return int(f), nil
}

// IntIdentifierExpr implements IntExpr... evaluates to some value in a tuple.
// (this is how we represent stuff like `job.id` in our AST)
type IntIdentifierExpr string

func (f IntIdentifierExpr) eval(r *Record) (int, error) {
	res, ok := (*r)[string(f)]
	if !ok {
		return 0, fmt.Errorf("identifier %s not defined in record %+v", f, r)
	}
	return res.raw.(int), nil
}
