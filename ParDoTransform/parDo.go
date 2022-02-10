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
	pcollection_numbers := beam.Create(scope, []int{1, 2, 3, 4, 5, 6, 7, 8, 9})

	// Print the PCollection
	beam.ParDo0(scope, func(numbers []int) {
		fmt.Println("Original List", numbers)
	}, pcollection_numbers)

	// Do a simple transformation on the data.
	/* Take each element of the input PCollection and continue with only the
	elements that are even and then multiply the resulting elements by 10 */
	pcollection_numbers = beam.ParDo(scope, FilterAndMultiply, pcollection_numbers)

	// Print the PCollection
	beam.ParDo0(scope, func(numbers []int) {
		fmt.Println("Transform List", numbers)
	}, pcollection_numbers)

	// "Run" invokes beam.Run with the runner supplied by the flag "runner".
	if err := beamx.Run(context.Background(), pipeline); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}

}

func FilterAndMultiply(numbers []int) []int {
	for i, v := range numbers {
		if math.Mod(float64(v), 2) == 0 {
			numbers[i] = numbers[i] * 10
		}
	}
	return numbers
}
