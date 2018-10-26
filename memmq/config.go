package memmq

import "conor.co.za/vservices/golib/logger"

var log = logger.New("mem-mq")

//Config to start a local-memory message queue
type Config struct {
	//no config...
}
