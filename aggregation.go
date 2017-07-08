package main

import (
	"io/ioutil"
	"log"
	"os"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

// Aggregation has aggregation condition
type Aggregation struct {
	Name string `yaml:"name"`
	ID   ID     `yaml:"id"`
	AND  AND    `yaml:"and,omitempty"`
	OR   OR     `yaml:"or,omitempty"`
}

// ID is to aggregate value
type ID struct {
	Type   string `yaml:"type"`
	Key    string `yaml:"key"`
	Column int    `yaml:"column,omitempty"`
}

type Condition interface {
	match(query map[string]string, headers []string) bool
}

// AND is and condition
type AND struct {
	Type   string     `yaml:"type"`
	Params []ParamMap `yaml:"params"`
}

func (a *AND) match(query map[string]string, headers []string) bool {
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

// OR is or condition
type OR struct {
	Type   string     `yaml:"type"`
	Params []ParamMap `yaml:"params"`
}

func (a *OR) match(query map[string]string, headers []string) bool {
	switch a.Type {
	case "query":
		for _, param := range a.Params {
			v, ok := query[param.Key]
			if ok != true {
				continue
			}
			if v == param.Value {
				return true
			}
		}
		return false
	case "header":
		for _, v := range a.Params {
			keyValue := strings.Split(headers[v.Column], "=")
			if v.Key != keyValue[0] {
				log.Println("log format error")
				continue
			}
			if v.Value == keyValue[1] {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// ParamMap is and/or key-value condition
type ParamMap struct {
	Column int    `yaml:"column,omitempty"`
	Key    string `yaml:"key"`
	Value  string `yaml:"value"`
}

func readCondition(path string) ([]Aggregation, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var aggregations []Aggregation
	err = yaml.Unmarshal(body, &aggregations)
	if err != nil {
		return nil, err
	}
	return aggregations, nil
}
