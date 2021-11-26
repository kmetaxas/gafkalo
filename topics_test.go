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
