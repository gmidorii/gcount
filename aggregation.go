package main

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"net/url"

	yaml "gopkg.in/yaml.v2"
)

// Aggregation has aggregation condition
type Aggregation struct {
	Name string `yaml:"name"`
	ID   ID     `yaml:"id"`
	AND  AND    `yaml:"and,omitempty"`
	OR   OR     `yaml:"or,omitempty"`
}

func (a *Aggregation) match(query url.Values, values []string) bool {
	if a.AND.Type != "" && a.OR.Type != "" {
		return a.AND.match(query, values) && a.OR.match(query, values)
	}
	if a.AND.Type != "" {
		return a.AND.match(query, values)
	}
	if a.OR.Type != "" {
		return a.OR.match(query, values)
	}
	return false
}

// ID is to aggregate value
type ID struct {
	Type   string `yaml:"type"`
	Key    string `yaml:"key"`
	Column int    `yaml:"column,omitempty"`
}

func (id *ID) extract(query url.Values, values []string) (string, error) {
	switch id.Type {
	case "query":
		vs, ok := query[id.Key]
		if ok != true {
			return "", errors.New("unexpected action: not found id key in query")
		}
		// first value
		return vs[0], nil
	case "header":
		keyValue := strings.Split(strings.Trim(values[id.Column], "\""), "=")
		if id.Key != keyValue[0] {
			return "", errors.New("unexpected action: not found id key in header")
		}
		return keyValue[1], nil
	default:
		return "", errors.New("unexpected id type: " + id.Type)
	}
}

type Condition interface {
	match(query map[string]string, headers []string) bool
}

// AND is and condition
type AND struct {
	Type   string     `yaml:"type"`
	Params []ParamMap `yaml:"params"`
}

func (a *AND) match(query url.Values, headers []string) bool {
	switch a.Type {
	case "query":
		for _, param := range a.Params {
			values, ok := query[param.Key]
			if ok != true {
				return false
			}
			// caution: multiple key
			for _, v := range values {
				if v != param.Value {
					return false
				}
			}
		}
		return true
	case "header":
		for _, v := range a.Params {
			keyValue := strings.Split(strings.Trim(headers[v.Column], "\""), "=")
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

func (a *OR) match(query url.Values, headers []string) bool {
	switch a.Type {
	case "query":
		for _, param := range a.Params {
			values, ok := query[param.Key]
			if ok != true {
				continue
			}
			// caution: multiple key
			for _, v := range values {
				if v == param.Value {
					return true
				}
			}
		}
		return false
	case "header":
		for _, v := range a.Params {
			keyValue := strings.Split(strings.Trim(headers[v.Column], "\""), "=")
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
	defer file.Close()

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
