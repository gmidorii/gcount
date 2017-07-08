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

	"github.com/deckarep/golang-set"
)

func worker(files []string, aggregations []Aggregation, wg *sync.WaitGroup) {
	start := time.Now()
	var names []string
	for _, v := range aggregations {
		names = append(names, v.Name)
	}
	for _, v := range files {
		work(v, names, aggregations)
	}
	end := time.Now()
	fmt.Printf("%f s\n", end.Sub(start).Seconds())
	wg.Done()
}

func work(v string, names []string, aggregations []Aggregation) {
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
		idmap, err := check(line, aggregations)
		if err != nil {
			log.Println(err)
			continue
		}
		if idmap == nil {
			continue
		}
		for key, value := range idmap {
			existSet, ok := resultMap[key]
			if ok != true {
				// initialize
				set := mapset.NewSet()
				set.Add(value)
				resultMap[key] = set
				continue
			}
			existSet.Add(value)
		}
	}
}

func containNames(str string, names []string) bool {
	for _, name := range names {
		if strings.Contains(str, name) {
			return true
		}
	}
	return false
}

func check(line string, aggregations []Aggregation) (map[string]string, error) {
	values := strings.Split(line, " ")
	u, err := url.Parse(values[6])
	if err != nil {
		return nil, err
	}
	// /xxx/key/hoge/hoge?
	paths := strings.SplitN(u.Path, "/", 4)
	// key
	key := paths[2]
	// hoge/hoge
	api := paths[3]
	for _, v := range aggregations {
		if api == v.Name {
			if !v.match(u.Query(), values) {
				break
			}
			id, err := v.ID.extract(u.Query(), values)
			if err != nil {
				return nil, err
			}
			return map[string]string{key: id}, nil
		}
	}
	// not match in condition
	return nil, nil
}
