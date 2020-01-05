package main

import (
)

type CountExecNode struct {
	Input ExecNode
}

func (c CountExecNode) Results() (chan *Record, error) {
	in, err := c.Input.Results()
	if err != nil {
		return nil, err
	}

	out := make(chan *Record)
	go func() {
		i := 0
		for _ = range in {
			i += 1
		}
		result := Record{}
		result["COUNT"] = IntVal(i)
		out <- &result
		close(out)
	}()

	return out, nil
}
