// +build ignore

package main

import (
	"fmt"
	"github.com/nsf/jsondiff"
)

// This file demonstrates how the JSON comparison works
func main() {
	// Example 1: Same JSON content, different formatting
	json1 := `{ "topicFilters": [ {"name": "TROL",  "patternType": "PREFIXED",  "filterType": "INCLUDE"} ] }`
	json2 := `{"topicFilters":[{"name":"TROL","patternType":"PREFIXED","filterType":"INCLUDE"}]}`
	
	fmt.Println("Example 1: Same content, different formatting")
	fmt.Printf("JSON 1: %s\n", json1)
	fmt.Printf("JSON 2: %s\n", json2)
	fmt.Printf("Result: %v\n\n", compareConfigValues("test", json1, json2))
	
	// Example 2: Different JSON content
	json3 := `{ "topicFilters": [ {"name": "TROL",  "patternType": "PREFIXED",  "filterType": "INCLUDE"} ] }`
	json4 := `{ "topicFilters": [ {"name": "DIFFERENT",  "patternType": "PREFIXED",  "filterType": "INCLUDE"} ] }`
	
	fmt.Println("Example 2: Different content")
	fmt.Printf("JSON 3: %s\n", json3)
	fmt.Printf("JSON 4: %s\n", json4)
	fmt.Printf("Result: %v\n\n", compareConfigValues("test", json3, json4))
	
	// Example 3: One JSON, one non-JSON
	json5 := `{ "topicFilters": [ {"name": "TROL",  "patternType": "PREFIXED",  "filterType": "INCLUDE"} ] }`
	notJson := `not a json string`
	
	fmt.Println("Example 3: JSON vs non-JSON")
	fmt.Printf("JSON 5: %s\n", json5)
	fmt.Printf("Not JSON: %s\n", notJson)
	fmt.Printf("Result: %v\n", compareConfigValues("test", json5, notJson))
}