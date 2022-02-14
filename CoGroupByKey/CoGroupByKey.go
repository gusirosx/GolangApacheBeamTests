package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

func init() {
	// Register element types and DoFns.
	beam.RegisterType(reflect.TypeOf(stringPair{}))
	//beam.RegisterFunction(SelectNumbers)
	beam.RegisterFunction(SplitStringPair)
	beam.RegisterFunction(formatCoGBKResults)
}

var emailSlice = []stringPair{
	{"Ady", "amy@example.com"},
	{"Carlos", "carlos@example.com"},
	{"Julia", "julia@example.com"},
	{"Carlos", "carlos@email.com"},
	{"Gustavo", "gustavo@email.com"},
}

var phoneSlice = []stringPair{
	{"Ady", "111-222-3333"},
	{"Jack", "222-333-4444"},
	{"Ady", "333-444-5555"},
	{"Carlos", "444-555-6666"},
	{"Gustavo", "555-666-7777"},
}

//Simple K,V structure
type stringPair struct {
	Key, Value string
}

// SplitStringPair is a helper function that splits a stringPair in a K,V structure
func SplitStringPair(p stringPair) (string, string) {
	return p.Key, p.Value
}

/* CreateAndSplit is a helper function that creates a PCollection in a
   K,V structure using the SplitStringPair function                    */
func CreateAndSplit(s beam.Scope, input []stringPair) beam.PCollection {
	return beam.ParDo(s, SplitStringPair, beam.CreateList(s, input))
}

/* CoGroupByKey Aggregates all input elements by their key and allows downstream processing
to consume all values associated with the key. While GroupByKey performs this operation over
a single input collection and thus a single type of input values, CoGroupByKey operates over
multiple input collections. As a result, the result for each key is a tuple of the values
associated with that key in each input collection. */

func main() {

	// "Parse" parses the command-line flags.
	flag.Parse()
	// beam.Init() is an initialization hook that must be called on startup.
	beam.Init()
	// create the Pipeline object and root scope.
	pipeline := beam.NewPipeline()
	scope := pipeline.Root()

	// Creating a PCollection

	// Do a simple transformation on the data.
	/* Take each element of the input PCollection and branch the odd and even elements */
	emails := CreateAndSplit(scope.Scope("CreateEmails"), emailSlice)
	phones := CreateAndSplit(scope.Scope("CreatePhones"), phoneSlice)

	// beam.ParDo0(scope, func(numbers string, value string) {
	// 	fmt.Println(numbers, value)
	// }, emails)

	// [START cogroupbykey_outputs]
	results := beam.CoGroupByKey(scope, emails, phones)
	// fmt.Println(results)
	// beam.ParDo0(scope, func(a, b, c []string) {
	// 	fmt.Println(a)
	// }, results)

	contactLines := beam.ParDo(scope, formatCoGBKResults, results)

	// Print the PCollection
	beam.ParDo0(scope, func(numbers string) {
		fmt.Println(numbers)
	}, contactLines)

	// "Run" invokes beam.Run with the runner supplied by the flag "runner".
	if err := beamx.Run(context.Background(), pipeline); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

// func SelectNumbers(numbers []int, emit_odd, emit_even func([]int)) {
// 	var odd, even []int
// 	for i, v := range numbers {
// 		if math.Mod(float64(v), 2) == 0 {
// 			even = append(even, numbers[i])
// 		} else {
// 			odd = append(odd, numbers[i])
// 		}
// 	}
// 	emit_odd(odd)
// 	emit_even(even)
// }

func formatCoGBKResults(key string, emailIter, phoneIter func(*string) bool) string {
	var s string
	var emails, phones []string
	//fmt.Println("-------->", emailIter(&s))
	for emailIter(&s) {
		//fmt.Println("--------<", emails)
		emails = append(emails, s)
	}
	for phoneIter(&s) {
		phones = append(phones, s)
	}
	// Values have no guaranteed order, sort for deterministic output.
	sort.Strings(emails)
	sort.Strings(phones)
	return fmt.Sprintf("%s; %s; %s", key, formatStringIter(emails), formatStringIter(phones))
}

func formatStringIter(vs []string) string {
	var b strings.Builder
	b.WriteRune('[')
	for i, v := range vs {
		b.WriteRune('\'')
		b.WriteString(v)
		b.WriteRune('\'')
		if i < len(vs)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteRune(']')
	return b.String()
}