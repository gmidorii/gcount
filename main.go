package main

import (
	"fmt"
	"log"
	"os"

	"io/ioutil"

	"path/filepath"
)

func main() {
	// in := flag.String("i", "", "input file directory")
	// out := flag.String("o", "", "output file directory")
	// flag.Parse()

	// set := mapset.NewSet()
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("err: %s", err)
	}
	aggregations, err := readCondition(filepath.Join(pwd, "conditions.yaml"))
	if err != nil {
		log.Fatalf("err: %s", err)
	}
	fmt.Println(aggregations)
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
