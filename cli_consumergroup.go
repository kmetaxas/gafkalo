package main

import (
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"log"
	"os"
)

type ConsumerGroupCmd struct {
	List     ConsumerGroupListCmd     `cmd help:"List consumer groups"`
	Describe ConsumerGroupDescribeCmd `cmd help:"Describe consumer group"`
}

type ConsumerGroupListCmd struct {
}
type ConsumerGroupDescribeCmd struct {
	Group string `help:"Group name to describe. If not specified all groups are described"`
}

// List consumer groups
func (cmd *ConsumerGroupListCmd) Run(ctx *CLIContext) error {

	config := LoadConfig(ctx.Config)
	admin, _, _ := GetAdminClients(config)
	groups, err := admin.AdminClient.ListConsumerGroups()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Groups: %s\n", groups)
	return nil
}

func (cmd *ConsumerGroupDescribeCmd) Run(ctx *CLIContext) error {
	var groups []string
	config := LoadConfig(ctx.Config)
	admin, _, _ := GetAdminClients(config)
	if cmd.Group != "" {
		groups = append(groups, cmd.Group)
	} else {
		existingGroups, err := admin.AdminClient.ListConsumerGroups()
		if err != nil {
			log.Fatal(err)
		}
		for groupName := range existingGroups {
			groups = append(groups, groupName)
		}
	}
	// now describe
	results, err := admin.AdminClient.DescribeConsumerGroups(groups)
	if err != nil {
		log.Fatal(err)
	}

	tb := table.NewWriter()
	rowConfigAutoMerge := table.RowConfig{AutoMerge: true}
	tb.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
		{Number: 2, AutoMerge: true},
		{Number: 3, AutoMerge: true},
		{Number: 4, AutoMerge: true},
		{Number: 5, AutoMerge: true},
		{Number: 6, AutoMerge: false},
		{Number: 7, AutoMerge: false},
	})
	tb.SetStyle(table.StyleLight)
	tb.Style().Options.SeparateRows = true
	tb.SetOutputMirror(os.Stdout)
	tb.AppendHeader(table.Row{"GroupID", "State", "ProtocolType", "Protocol", "Topic", "Partition", "Offset"}, rowConfigAutoMerge)
	for _, groupRes := range results {
		/*
			for member, memberDesc := range groupRes.Members {
				fmt.Printf("- Member: %s\n", member)
				fmt.Printf("\tClientId: %s , ClientHost: %s, MemberMetadata: [%s], MemberAssignment: [%s]\n", memberDesc.ClientId, memberDesc.ClientHost, memberDesc.MemberMetadata, memberDesc.MemberAssignment)
			}
		*/
		offsetResp, err := admin.AdminClient.ListConsumerGroupOffsets(groupRes.GroupId, nil)
		if err != nil {
			log.Fatal(err)
		}
		for topic, offsets := range offsetResp.Blocks {
			// Don't iterate of ofset as we want to sort it. Since its a map of int32 we can just do a normal for loop..
			for i := 0; i < len(offsets); i++ {
				tb.AppendRow(table.Row{groupRes.GroupId, groupRes.State, groupRes.ProtocolType, groupRes.Protocol, topic, i, offsets[int32(i)].Offset}, rowConfigAutoMerge)
			}
		}
	}
	tb.Render()

	return nil

}
