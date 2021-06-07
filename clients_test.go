package main

import "testing"

func getTestAdmin() *MDSAdmin {
	mdsAdmin := MDSAdmin{
		Url:                     "localhost:9093",
		SchemaRegistryClusterID: "schemaregistry",
		KafkaClusterID:          "clusterID",
		ConnectClusterId:        "connectID",
		KSQLClusterID:           "ksqlID",
	}
	return &mdsAdmin
}
func getTestMDSResourcePattern() MDSResourcePattern {
	pat1 := MDSResourcePattern{ResourceType: "Subject", Name: "TestTopic", PatternType: "PREFIXED"}
	return pat1
}
func getTestRolebindings() MDSRolebindings {
	res := MDSRolebindings{}
	res.DeveloperRead = append(res.DeveloperRead, getTestMDSResourcePattern())
	res.DeveloperWrite = append(res.DeveloperRead, getTestMDSResourcePattern())
	res.ResourceOwner = append(res.DeveloperRead, getTestMDSResourcePattern())
	return res
}

func TestGetContext(t *testing.T) {
	admin := getTestAdmin()
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

func Test_roleExists(t *testing.T) {
	admin := getTestAdmin()
	result := getTestClientResult()
	roles := getTestRolebindings()
	roleNames := []string{"DeveloperRead", "DeveloperWrite", "ResourceOwner"}
	for _, roleName := range roleNames {
		result.ResourceType = "Subject"
		result.PatternType = "PREFIXED"
		result.Role = roleName
		exists := admin.roleExists(result, roles)
		if !exists {
			t.Errorf("Pattern %s should exist in %s", result, roles)
		}
		result.PatternType = "LITERAL"
		exists = admin.roleExists(result, roles)
		if exists {
			t.Errorf("Pattern %s should NOT exist in %s", result, roles)
		}
	}

}

func Test_getKafkaClusterID(t *testing.T) {

}
