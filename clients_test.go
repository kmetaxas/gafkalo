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
