package natsmq

import "conor.co.za/vservices/golib/logger"

var log = logger.New("nats-mq")

//Config to start a NATS Streaming message queue
type Config struct {
	URL           string `json:"url" doc:"NATS URL (defaults to nats://localhost:4222)"`
	ClusterID     string `json:"clusterId" doc:"NATS Cluster Identifier (defaults to: ETL-<inst>)"`
	ClientID      string `json:"clientId" doc:"Unique ID used to connect to NATS (defaults to <host>-<stream>-<proc>-<prci>)"`
	ConsumerGroup string `json:"consumerGroup" doc:"Group of consumers working together. Messages goes to only one in the group."`
}
