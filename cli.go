package main

import (
		"fmt"
)

func fmtResults(resChan chan *Record) {
	i := 0
	for r := range resChan {
		fmt.Printf("***************** %3d **************\n", i)
		fmt.Printf("%s", r)
		i += 1
	}
	fmt.Println()
}
