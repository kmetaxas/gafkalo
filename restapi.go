package main

import (
	"net/http"

	"fmt"

	"github.com/Shopify/sarama"
	"github.com/labstack/echo/v4"
)

type GafkaloRestServer struct {
	srv         *echo.Echo
	adminclient *KafkaAdmin
	producer    *Producer
}

func (s *GafkaloRestServer) Start(p string) error {
	return s.srv.Start(p)

}

/*
Create a Kafka Topic
*/
func (s *GafkaloRestServer) createTopicHandler(c echo.Context) error {
	var topic Topic
	err := c.Bind(&topic)
	if err != nil {
		return err
	}
	detail := sarama.TopicDetail{
		NumPartitions:     topic.Partitions,
		ReplicationFactor: topic.ReplicationFactor,
		ConfigEntries:     topic.Configs,
	}
	err = s.adminclient.AdminClient.CreateTopic(topic.Name, &detail, false)
	if err != nil {
		fmt.Printf("Error from CreateTopic: %s\n", err)
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.String(http.StatusOK, fmt.Sprintf("Created topic %s", topic.Name))
}

func NewAPIServer(conf Configuration) *GafkaloRestServer {
	srv := echo.New()
	admin := NewKafkaAdmin(conf.Connections.Kafka)
	gServer := &GafkaloRestServer{
		srv:         srv,
		adminclient: &admin,
	}
	//Register handlers
	srv.GET("/trololo", gServer.createTopicHandler)
	return gServer
}
