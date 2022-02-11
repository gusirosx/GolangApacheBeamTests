package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

func init() {
	beam.RegisterFunction(SelectNumbers)
}

func main() {

	// "Parse" parses the command-line flags.
	flag.Parse()
	// beam.Init() is an initialization hook that must be called on startup.
	beam.Init()

	// create the Pipeline object and root scope.
	pipeline := beam.NewPipeline()
	scope := pipeline.Root()

	// Creating a PCollection
	pcollection_numbers := beam.Create(scope, []int{1, 2, 3, 4, 5, 6, 7, 8, 9})

	// Print the PCollection
	beam.ParDo0(scope, func(numbers []int) {
		fmt.Println("Original List", numbers)
	}, pcollection_numbers)

	// Do a simple transformation on the data.
	/* Take each element of the input PCollection and branch the odd and even elements */
	odd, even := beam.ParDo2(scope, SelectNumbers, pcollection_numbers)
	// Print the PCollection
	beam.ParDo0(scope, func(numbers []int) {
		fmt.Println("Odd List", numbers)
	}, odd)
	beam.ParDo0(scope, func(numbers []int) {
		fmt.Println("Even List", numbers)
	}, even)

	// "Run" invokes beam.Run with the runner supplied by the flag "runner".
	if err := beamx.Run(context.Background(), pipeline); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

func SelectNumbers(numbers []int, emit_odd, emit_even func([]int)) {
	var odd, even []int
	for i, v := range numbers {
		if math.Mod(float64(v), 2) == 0 {
			even = append(even, numbers[i])
		} else {
			odd = append(odd, numbers[i])
		}
	}
	emit_odd(odd)
	emit_even(even)
}
