package main

import (
	"fmt"
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

type GafkaloRestServer struct {
	srv         *echo.Echo
	adminclient *KafkaAdmin
	producer    *Producer
}

func (s *GafkaloRestServer) Start(p string) error {
	return s.srv.Start(p)

}

type restApiGenericResponse struct {
	Result string `json:"result"`
	Error  string `json:"errors"`
}

/*
Create a Kafka Topic
*/
func (s *GafkaloRestServer) createTopicHandler(c echo.Context) error {
	var topic Topic

	err := c.Bind(&topic)
	if err != nil {
		return c.String(http.StatusBadRequest, "bad request")
	}
	detail := sarama.TopicDetail{
		NumPartitions:     topic.Partitions,
		ReplicationFactor: topic.ReplicationFactor,
		ConfigEntries:     topic.Configs,
	}
	err = s.adminclient.AdminClient.CreateTopic(topic.Name, &detail, false)
	if err != nil {
		log.Errorf("Error from CreateTopic: %s\n", err)
		return c.JSON(http.StatusInternalServerError, &restApiGenericResponse{Result: "Failed to create topic", Error: err.Error()})
	}

	return c.JSON(http.StatusOK, &restApiGenericResponse{Result: fmt.Sprintf("Created topic %s", topic.Name), Error: ""})
}

/*
Delete a  Kafka Topic
*/
func (s *GafkaloRestServer) deleteTopicHandler(c echo.Context) error {
	type DeleteTopicRequest struct {
		Topic string `json:"topic"`
	}
	var topic DeleteTopicRequest

	err := c.Bind(&topic)
	if err != nil {
		return c.String(http.StatusBadRequest, "bad request")
	}
	log.Errorf("Deleting topic %v", topic)
	err = s.adminclient.DeleteTopic(topic.Topic)
	if err != nil {
		log.Errorf("Error from deleteTopicHandler (topic: %s): %s\n", topic.Topic, err)
		return c.JSON(http.StatusInternalServerError, &restApiGenericResponse{Result: fmt.Sprintf("Failed to delete topic %s", topic.Topic), Error: err.Error()})
	}

	return c.JSON(http.StatusOK, &restApiGenericResponse{Result: fmt.Sprintf("Deleted topic %s", topic.Topic), Error: ""})
}
func NewAPIServer(conf Configuration) *GafkaloRestServer {
	srv := echo.New()
	admin := NewKafkaAdmin(conf.Connections.Kafka)
	gServer := &GafkaloRestServer{
		srv:         srv,
		adminclient: &admin,
	}
	//Register handlers
	srv.POST("/topic/create", gServer.createTopicHandler)
	srv.POST("/topic/delete", gServer.deleteTopicHandler)
	return gServer
}
