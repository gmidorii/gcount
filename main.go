package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"io/ioutil"

	"path/filepath"
	"sync"

	"github.com/deckarep/golang-set"
)

var resultSet = mapset.NewSet()

func main() {
	in := flag.String("i", "./input", "input file directory")
	// out := flag.String("o", "", "output file directory")
	routine := flag.Int("g", 3, "goroutine number")
	flag.Parse()

	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("err: %s", err)
	}
	aggregations, err := readCondition(filepath.Join(pwd, "conditions.yaml"))
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

	fmt.Println(resultSet.String())
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
