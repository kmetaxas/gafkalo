package main

import (
	"reflect"
	"testing"
)

func TestParseOffsetsArg(t *testing.T) {
	var expected map[int32]int64
	var param string
	expected = make(map[int32]int64)
	param = "TOPIC=0:4,1:300,2:5124"
	res, err := parseOffsetsArg(&param)
	if err != nil {
		t.Error(err)
	}
	expected[0], expected[1], expected[2] = 4, 300, 5124
	if !reflect.DeepEqual(&res, &expected) {
		t.Errorf("%+vs not equal to %+vs", res, expected)
	}
	// now break it
	param = "TOPIC0:4,1:300,2:5124"
	res, err = parseOffsetsArg(&param)
	if err == nil {
		t.Error("Should get error about wrong structure")
	}
	param = "TOPIC=0:4,,1:300,2:5124"
	res, err = parseOffsetsArg(&param)
	if err == nil {
		t.Error("Should get error about wrong structure")
	}
}
