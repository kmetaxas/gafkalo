package main

import (
	"fmt"
)

func testClusterLinkChanges() {
	// Simulate the scenario from the user's example
	admin := &ClusterLinkAdmin{}

	// Current config from REST API
	current := &ClusterLink{
		Name: "trololo2",
		Configs: map[string]string{
			"auto.create.mirror.topics.filters": `{ "topicFilters": [ {"name": "TROL",  "patternType": "PREFIXED",  "filterType": "INCLUDE"} ] }`,
			"remote.link.connection.mode":       "INBOUND", // This would now be filtered out as read-only
		},
	}

	// New config from YAML
	new := &ClusterLink{
		Name: "trololo2",
		Configs: map[string]string{
			"auto.create.mirror.topics.filters": `{ "topicFilters": [ {"name": "TROL",  "patternType": "PREFIXED",  "filterType": "INCLUDE"} ] }`,
		},
	}

	hasChanged, diff, err := admin.NeedsUpdate(current, new)

	fmt.Printf("Test Results:\n")
	fmt.Printf("Has Changed: %v\n", hasChanged)
	fmt.Printf("Error: %v\n", err)
	if diff != nil {
		fmt.Printf("Changed Configs: %d\n", len(diff.ChangedConfigs))
		for key, change := range diff.ChangedConfigs {
			fmt.Printf("  - %s: %v -> %v\n", key, SafeNullStr(change.OldValue), SafeNullStr(change.NewValue))
		}
	}
}

