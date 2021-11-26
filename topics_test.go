package main

import (
	"testing"
)

func TestCalculatePartitionPlan(t *testing.T) {

	// 3 partitions of replication factor 4, with a 6 broker cluster
	plan1, err := calculatePartitionPlan(3, 6, 4, nil)
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
		for _, num := range partition {
			if num < 1 || num > 6 {
				t.Error("Broker IDs can't be less than 1 or more than 6 in a cluster of 6 brokers")
			}
		}
		// TODO Test for duplicate broker IDs
	}
	// 12 partitions of replication factor 6, with a 3 broker cluster
	// this represents an impossible combination
	plan2, err := calculatePartitionPlan(12, 3, 6, nil)
	if err == nil {
		t.Error("Should raise error about brokers being less than replication factor")
	}
	if len(plan2) != 0 {
		t.Error("Plan2 should be empty")
	}
	// TODO test with passing oldPlan!
}

func TestRandNonRpeatingIntSet(t *testing.T) {
	s, err := randNonRepeatingIntSet(1, 10, 3)
	if err != nil {
		t.Error(err)
	}
	if len(s) != 3 {
		t.Error("Returned set length not 3")
	}
	// check for duplicates
	for _, num := range s {
		seen := make(map[int]bool)
		if _, ok := seen[num]; ok {
			t.Errorf("Key %d already exists!", num)
		} else {
			seen[num] = true
		}

	}
	s, err = randNonRepeatingIntSet(1, 5, 10)
	if err == nil {
		t.Error("Should return error about available space for random set")
	}
	s, _ = randNonRepeatingIntSet(-5, 1, 5)
	if err == nil {
		t.Error("Must raise error about negative from")
	}
}
