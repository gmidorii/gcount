package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
)

func worker(files []string, aggregations []Aggregation) {
	for _, v := range files {
		f, err := os.Open(v)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		r, err := gzip.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}
		defer r.Close()

		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
	}
}
