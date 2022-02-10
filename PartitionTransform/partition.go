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
	beam.RegisterFunction(FilterAndMultiply)
	beam.RegisterFunction(SelectNumber)
}

func main() {

	// "Parse" parses the command-line flags. Must be called after all flags are defined and before flags are accessed by the program.
	flag.Parse()
	// beam.Init() is an initialization hook that must be called on startup.
	beam.Init()

	// create the Pipeline object and root scope.
	pipeline := beam.NewPipeline()
	scope := pipeline.Root()

	// Creating a PCollection
	pcollection_numbers := beam.CreateList(scope, []int{1, 2, 3, 4, 5, 6, 7, 8, 9})
	// Partition returns a slice of PCollections
	numbers_partition := beam.Partition(scope, 2, SelectNumber, pcollection_numbers)

	// Print the []PCollection
	// for i, v := range numbers_partition {
	// 	PrintParDo(scope, v, i)
	// }

	// Print the PCollection
	beam.ParDo0(scope, func(numbers int) {
		fmt.Println(numbers)
	}, numbers_partition[1])

	// "Run" invokes beam.Run with the runner supplied by the flag "runner".
	if err := beamx.Run(context.Background(), pipeline); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}

}

func PrintParDo(scope beam.Scope, pcol beam.PCollection, index int) {
	beam.ParDo0(scope, func(test int) {
		fmt.Println(index, test)
	}, pcol)
}

func FilterAndMultiply(numbers []int) []int {
	for i, v := range numbers {
		if math.Mod(float64(v), 2) == 0 {
			numbers[i] = numbers[i] * 10
		}
	}
	return numbers
}

func SelectNumber(number int) int {
	return number % 2
}
