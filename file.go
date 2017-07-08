package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
)

func worker(files []string, aggregations []Aggregation) {
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
			check(line)
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

func check(line string) bool {
	values := strings.Split(line, " ")
	u, err := url.Parse(values[6])
	if err != nil {
		log.Fatal(err)
		return false
	}
	// /xxx/xxx/hoge/hoge -> hoge/hoge
	api := strings.SplitN(u.Path, "/", 4)[3]
	fmt.Println(api)
	for key, values := range u.Query() {
		for _, v := range values {
			fmt.Println(key)
			fmt.Println(v)
		}
	}
	return false
}
