package main

type ExecNode interface {
	Results() (chan *Record, error)
}
