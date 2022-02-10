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
	beam.RegisterFunction(decileFn)
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
	//pcollection_text := beam.Create(scope, []string{"Hello", "World", "from", "Apache", "Beam"})
	//scope.Scope("teste")

	// Print the PCollection
	beam.ParDo0(scope, func(numbers []int) {
		fmt.Println(numbers)
	}, pcollection_numbers)

	// do a simple transformation on the data.
	// pcollection_numbers = beam.ParDo(scope, func(numbers []int) []int {
	// 	numbers[1] = 10

	// 	return numbers
	// }, pcollection_numbers)

	/* Take each element of the input PCollection and continue with only the
	elements that are even and then multiply the resulting elements by 10 */
	pcollection_numbers = beam.ParDo(scope, FilterAndMultiply, pcollection_numbers)

	// Print the PCollection
	beam.ParDo0(scope, func(numbers []int) {
		fmt.Println(numbers)
	}, pcollection_numbers)

	// picoles := beam.Partition(scope, 2, SelectNumber, pcollection_numbers)

	students := beam.CreateList(scope, []Student{{42}, {57}, {23}, {89}, {99}, {5}, {8}, {10}, {11}, {15}, {18}})
	// Partition returns a slice of PCollections
	studentsByPercentile := beam.Partition(scope, 40, decileFn, students)

	// beam.ParDo0(scope, func(test Student) {
	// 	fmt.Println(test)
	// }, students)

	for _, v := range studentsByPercentile {
		PrintPicole(scope, v)
	}

	// avg := applyPartition(s, input)
	// passert.Equals(s, avg, Student{42})
	// ptest.RunAndValidate(t, p)

	// "Run" invokes beam.Run with the runner supplied by the flag "runner".
	if err := beamx.Run(context.Background(), pipeline); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}

}

func PrintPicole(scope beam.Scope, picole beam.PCollection) {
	beam.ParDo0(scope, func(test Student) {
		fmt.Println(test)
	}, picole)
}

type Student struct {
	Percentile int
}

func decileFn(student Student) int {
	return int(float64(student.Percentile) / float64(10))
}

// applyPartition returns the 40th percentile of students.
func ApplyPartition(s beam.Scope, students beam.PCollection) beam.PCollection {
	// [START model_multiple_pcollections_partition]
	// Partition returns a slice of PCollections
	studentsByPercentile := beam.Partition(s, 10, decileFn, students)
	// Each partition can be extracted by indexing into the slice.
	fortiethPercentile := studentsByPercentile[4]
	// [END model_multiple_pcollections_partition]
	return fortiethPercentile
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
	if math.Mod(float64(number), 2) == 0 {
		return number
	}
	return 0

}
