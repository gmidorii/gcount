package main

import (
	"flag"
	"fmt"
	"log"

	"io/ioutil"

	"path/filepath"
	"sync"

	"os"

	"github.com/deckarep/golang-set"
)

var resultMap = map[string]mapset.Set{}

func main() {
	in := flag.String("i", "./input", "input file directory")
	out := flag.String("o", "./output", "output file directory")
	routine := flag.Int("g", 3, "goroutine number")
	conditions := flag.String("c", "./conditions.yaml", "aggregation condition file (.yaml)")
	flag.Parse()

	aggregations, err := readCondition(*conditions)
	if err != nil {
		log.Fatalf("err: %s", err)
	}

	files, err := getAllFilePath(*in)
	if err != nil {
		log.Fatalf("err: %s", err)
	}
	divided := chunk(files, *routine)

	var wg sync.WaitGroup
	for i := 0; i < *routine; i++ {
		wg.Add(1)
		go worker(divided[i], aggregations, &wg)
	}
	wg.Wait()

	for key, set := range resultMap {
		file, err := os.Create(filepath.Join(*out, key))
		if err != nil {
			log.Fatalf("err: %s", err)
		}
		defer file.Close()
		for _, value := range set.ToSlice() {
			file.WriteString(fmt.Sprintf("%v\n", value))
		}
	}
}

func getAllFilePath(input string) ([]string, error) {
	files, err := ioutil.ReadDir(input)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		paths = append(paths, filepath.Join(input, file.Name()))
	}
	return paths, nil
}

func chunk(slice []string, num int) [][]string {
	var divided [][]string

	size := (len(slice) + num - 1) / num
	for i := 0; i < len(slice); i += size {
		end := i + size
		if end > len(slice) {
			end = len(slice)
		}

		divided = append(divided, slice[i:end])
	}
	return divided
}
