package main

import (
	"flag"
	"log"

	"io/ioutil"

	"path/filepath"

	"strings"

	"github.com/deckarep/golang-set"
)

type Aggregation struct {
	Name       string
	ID         ID
	Conditions []Condition
}

type ID struct {
	Type   string
	Key    string
	Column int
}

type Condition interface {
	match(query map[string]string, headers []string) bool
}

type And struct {
	Type   string
	Params []ParamMap
}

func (a *And) match(query map[string]string, headers []string) bool {
	switch a.Type {
	case "query":
		for _, param := range a.Params {
			v, ok := query[param.Key]
			if ok != true {
				return false
			}
			if v != param.Value {
				return false
			}
		}
		return true
	case "header":
		for _, v := range a.Params {
			keyValue := strings.Split(headers[v.Column], "=")
			if v.Key != keyValue[0] {
				log.Println("log format error")
				return false
			}
			if v.Value != keyValue[1] {
				return false
			}
		}
		return true
	default:
		return false
	}
}

type Or struct {
	Type   string
	Column int
	Params []ParamMap
}

type ParamMap struct {
	Column int
	Key    string
	Value  string
}

func main() {
	in := flag.String("i", "", "input file directory")
	out := flag.String("o", "", "output file directory")
	flag.Parse()

	set := mapset.NewSet()
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
