package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

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
	pcollection_text := beam.Create(scope, []string{"Hello", "World", "from", "Apache", "Beam"})
	pcollection_numbers.Type()

	// Print the PCollection
	beam.ParDo0(scope, func(numbers []int) {
		fmt.Println(numbers)
	}, pcollection_numbers)
	beam.ParDo0(scope, func(text []string) {
		fmt.Println(text)
	}, pcollection_text)

	// "Run" invokes beam.Run with the runner supplied by the flag "runner".
	if err := beamx.Run(context.Background(), pipeline); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}

}
