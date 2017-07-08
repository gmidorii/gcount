package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

func worker(files []string, aggregations []Aggregation, wg *sync.WaitGroup) {
	start := time.Now()
	var names []string
	for _, v := range aggregations {
		names = append(names, v.Name)
	}
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
			line := scanner.Text()
			if !containNames(line, names) {
				continue
			}
			id, err := check(line, aggregations)
			if err != nil {
				log.Println(err)
				continue
			}
			if id == "" {
				continue
			}
			resultSet.Add(id)
		}
	}
	end := time.Now()
	fmt.Printf("%f s\n", end.Sub(start).Seconds())
	wg.Done()
}

func containNames(str string, names []string) bool {
	for _, name := range names {
		if strings.Contains(str, name) {
			return true
		}
	}
	return false
}

func check(line string, aggregations []Aggregation) (string, error) {
	values := strings.Split(line, " ")
	u, err := url.Parse(values[6])
	if err != nil {
		return "", err
	}
	// /xxx/xxx/hoge/hoge -> hoge/hoge
	api := strings.SplitN(u.Path, "/", 4)[3]
	for _, v := range aggregations {
		if api == v.Name {
			if !v.match(u.Query(), values) {
				break
			}
			return v.ID.extract(u.Query(), values)
		}
	}
	// not match in condition
	return "", nil
}
