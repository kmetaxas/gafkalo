package main

import "testing"

func getAdmin() *MDSAdmin {
	mdsAdmin := MDSAdmin{
		Url:                     "localhost:9093",
		SchemaRegistryClusterID: "schemaregistry",
		KafkaClusterID:          "clusterID",
		ConnectClusterId:        "connectID",
		KSQLClusterID:           "ksqlID",
	}
	return &mdsAdmin
}

func TestGetContext(t *testing.T) {
	admin := getAdmin()
	ctx := admin.getContext(CTX_KAFKA)
	if ctx.Clusters["kafka-cluster"] != "clusterID" || ctx.Clusters["schema-registry-cluster"] != "" || ctx.Clusters["ksql-cluster"] != "" || ctx.Clusters["connect-cluster"] != "" {
		t.Error("Wrong Cluster context for CTX_KAFKA")
	}
	ctx = admin.getContext(CTX_SR)
	if ctx.Clusters["kafka-cluster"] != "clusterID" || ctx.Clusters["schema-registry-cluster"] != "schemaregistry" || ctx.Clusters["ksql-cluster"] != "" || ctx.Clusters["connect-cluster"] != "" {
		t.Error("Wrong Cluster context for CTX_SR")
	}
	ctx = admin.getContext(CTX_KSQL)
	if ctx.Clusters["kafka-cluster"] != "clusterID" || ctx.Clusters["schema-registry-cluster"] != "" || ctx.Clusters["ksql-cluster"] != "ksqlID" || ctx.Clusters["connect-cluster"] != "" {
		t.Error("Wrong Cluster context for CTX_KSQL")
	}
	ctx = admin.getContext(CTX_CONNECT)
	if ctx.Clusters["kafka-cluster"] != "clusterID" || ctx.Clusters["schema-registry-cluster"] != "" || ctx.Clusters["ksql-cluster"] != "" || ctx.Clusters["connect-cluster"] != "connectID" {
		t.Error("Wrong Cluster context for CTX_CONNECT")
	}
}

func TestGetPrefixStr(t *testing.T) {
	if res := getPrefixStr(true); res != "LITERAL" {
		t.Error("getPrefixStr wrong value (should be LITERAL for true")
	}
	if res := getPrefixStr(false); res != "PREFIXED" {
		t.Error("getPrefixStr wrong value (should be PREFIXED for false")
	}

}

func TestCompareResultWithResourcePatterns(t *testing.T) {
	result1 := ClientResult{Principal: "User:test", ResourceType: "Topic", ResourceName: "TestTopic", Role: "DeveloperRead", PatternType: "PREFIXED"}
	var existsPatterns []MDSResourcePattern
	pat1 := MDSResourcePattern{ResourceType: "Subject", Name: "TestTopic", PatternType: "PREFIXED"}
	patsame := MDSResourcePattern{ResourceType: "Topic", Name: "TestTopic", PatternType: "PREFIXED"}
	existsPatterns = append(existsPatterns, pat1)
	existsPatterns = append(existsPatterns, patsame)
	found := compareResultWithResourcePatterns(result1, existsPatterns)
	if !found {
		t.Error("Result not found in MDSResourcePattern list")
	}
	var notExistsPatterns []MDSResourcePattern
	notExistsPatterns = append(notExistsPatterns, pat1)
	found = compareResultWithResourcePatterns(result1, notExistsPatterns)
	if found {
		t.Error("Result should not be in MDSResourcePattern list")
	}
}
