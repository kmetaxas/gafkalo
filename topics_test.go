package main

import (
	"testing"
)

func TestCalculatePartitionPlan(t *testing.T) {

	brokerIDs := []int32{0, 1, 3, 4, 5, 6}
	// 3 partitions of replication factor 4, with a 6 broker cluster
	plan1, err := calculatePartitionPlan(3, 6, 4, brokerIDs, nil)
	if err != nil {
		t.Error(err)
	}
	if len(plan1) != 3 {
		t.Error("Plan should have 3 partitions!")
	}
	for _, partition := range plan1 {
		if len(partition) != 4 {
			t.Errorf("Partition: %v does not have replication factor 3", partition)
		}
		// TODO Test for duplicate broker IDs
	}
	// 12 partitions of replication factor 6, with a 3 broker cluster
	// this represents an impossible combination
	plan2, err := calculatePartitionPlan(12, 3, 6, brokerIDs, nil)
	if err == nil {
		t.Error("Should raise error about brokers being less than replication factor")
	}
	if len(plan2) != 0 {
		t.Error("Plan2 should be empty")
	}
	// TODO test with passing oldPlan!
}

func TestRandNonRpeatingIntSet(t *testing.T) {
	brokerList := []int32{0, 2, 3, 5}
	s, err := randNonRepeatingIntSet(brokerList, 3)
	if err != nil {
		t.Error(err)
	}
	if len(s) != 3 {
		t.Error("Returned set length not 3")
	}
	// check for duplicates
	for _, num := range s {
		seen := make(map[int32]bool)
		if _, ok := seen[num]; ok {
			t.Errorf("Key %d already exists!", num)
		} else {
			seen[num] = true
		}

	}
	_, err = randNonRepeatingIntSet(brokerList, 10)
	if err == nil {
		t.Error("Should return error about available space for random set")
	}
}

func TesttopicPartitionNeedsUpdate(t *testing.T) {
	newTopic := Topic{
		Name:       "trololo",
		Partitions: 5,
	}
	existing := Topic{
		Name:       "trololo",
		Partitions: 10,
	}
	if topicPartitionNeedUpdate(newTopic, existing) != true {
		t.Error("topicPartitionNeedUpdate is false but should be true")
	}
}
