package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*boundedSum)(nil)))
}

func sumInts(a, v int) int {
	return a + v
}

func init() {
	beam.RegisterFunction(sumInts)
}

func main() {

	// "Parse" parses the command-line flags.
	flag.Parse()
	// beam.Init() is an initialization hook that must be called on startup.
	beam.Init()
	// create the Pipeline object and root scope.
	pipeline := beam.NewPipeline()
	scope := pipeline.Root()

	// // Do a simple transformation on the data.
	// emails := CreateAndSplit(scope.Scope("CreateEmails"), emailSlice)
	// phones := CreateAndSplit(scope.Scope("CreatePhones"), phoneSlice)
	// // beam.ParDo0(scope, func(name string, value string) {
	// // 	fmt.Println(name, value)
	// // }, emails)
	// results := beam.CoGroupByKey(scope, emails, phones)
	// contactLines := beam.ParDo(scope, formatCoGBKResults, results)

	// // Print the PCollection
	// beam.ParDo0(scope, func(numbers string) {
	// 	fmt.Println(numbers)
	// }, contactLines)

	input := beam.CreateList(scope, []int{1, 2, 3})
	avg := globallySumInts(scope, input)
	beam.ParDo0(scope, func(numbers int) {
		fmt.Println(numbers)
	}, avg)

	// "Run" invokes beam.Run with the runner supplied by the flag "runner".
	if err := beamx.Run(context.Background(), pipeline); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

func globallySumInts(s beam.Scope, ints beam.PCollection) beam.PCollection {
	return beam.Combine(s, sumInts, ints)
}

type boundedSum struct {
	Bound int
}

func (fn *boundedSum) MergeAccumulators(a, v int) int {
	sum := a + v
	if fn.Bound > 0 && sum > fn.Bound {
		return fn.Bound
	}
	return sum
}

func globallyBoundedSumInts(s beam.Scope, bound int, ints beam.PCollection) beam.PCollection {
	return beam.Combine(s, &boundedSum{Bound: bound}, ints)
}

// [END combine_simple_sum]
