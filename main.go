package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"

	"io/ioutil"

	"path/filepath"
	"sync"

	"os"

	"net/http"
	_ "net/http/pprof"

	"github.com/deckarep/golang-set"
	"golang.org/x/sync/syncmap"
)

var resultMap = syncmap.Map{}

func main() {
	// pprof
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// flag
	in := flag.String("i", "./input", "input file directory")
	out := flag.String("o", "./output", "output file directory")
	conditions := flag.String("c", "./conditions.yaml", "aggregation condition file (.yaml)")
	flag.Parse()

	// setup
	aggregations, err := readCondition(*conditions)
	if err != nil {
		log.Fatalf("err: %s", err)
	}
	files, err := getAllFilePath(*in)
	if err != nil {
		log.Fatalf("err: %s", err)
	}
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	divided := chunk(files, cpus)
	var wg sync.WaitGroup
	semaphone := make(chan int, cpus)
	for i := 0; i < len(divided); i++ {
		wg.Add(1)
		go worker(divided[i], aggregations, &wg, semaphone)
	}
	wg.Wait()

	// output
	resultMap.Range(func(key, set interface{}) bool {
		file, err := os.Create(filepath.Join(*out, fmt.Sprintf("%v", key)))
		if err != nil {
			return false
		}
		defer file.Close()
		for _, value := range set.(mapset.Set).ToSlice() {
			file.WriteString(fmt.Sprintf("%v\n", value))
		}
		return true
	})
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
