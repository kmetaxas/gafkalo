package main

import (
	"fmt"
	"testing"
)

func TestGetSubjectForTopic(t *testing.T) {

	topic := "TOPIC1"
	expected_value := fmt.Sprintf("%s-value", topic)
	expected_key := fmt.Sprintf("%s-key", topic)

	test1 := getSubjectForTopic(topic, false)
	if test1 != expected_value {
		t.Errorf("%s != %s", test1, expected_value)
	}
	test2 := getSubjectForTopic(topic, true)
	if test2 != expected_key {
		t.Errorf("%s != %s", test2, expected_key)
	}

}
