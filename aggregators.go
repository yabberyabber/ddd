package main

func countAll(in chan *Record) (chan *Record, error) {
	out := make(chan *Record)
	go func() {
		i := 0
		for _ = range in {
			i += 1
		}
		result := Record{}
		result["COUNT"] = Val{
			raw: i,
			meta: &IntTypeMeta,
		}
		out <- &result
		close(out)
	}()
	return out, nil
}
