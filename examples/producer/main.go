package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/jansemmelink/mq"
	"github.com/jansemmelink/mq/memmq"
	"github.com/jansemmelink/mq/natsmq"
	"github.com/jansemmelink/mq/rabbitmq"
	"github.com/stewelarend/logger"
)

var log = logger.New()

func main() {
	topicFlag := flag.String("t", "test", "Topic")
	nrFlag := flag.Int("n", 10, "Nr of messages to product")
	delaySecondsFlag := flag.Int("T", 0, "delay in seconds between messages")
	debugFlag := flag.Bool("d", false, "Run with DEBUG output")
	mqFlag := flag.String("mq", "mem", "Type of mq: mem|nats|rabbit")
	flag.Parse()
	if *debugFlag {
		log = log.WithLevel(logger.LevelDebug)
	}

	log.Debugf("Connecting...")
	var broker mq.IBroker
	switch *mqFlag {
	case "mem":
		config := memmq.Config{}
		broker = memmq.New(config)
	case "nats":
		config := natsmq.Config{
			URL:           "nats://localhost:4222",
			ClusterID:     "ETL",
			ClientID:      "exampleClient",
			ConsumerGroup: "exampleGroup",
		}
		broker = natsmq.New(config)
	case "rabbit":
		config := rabbitmq.Config{}
		var err error
		broker, err = rabbitmq.New(config)
		if err != nil {
			panic(err)
		}
	default:
		panic(fmt.Errorf("Invalid --mq=%s", *mqFlag))
	}
	defer broker.Close()

	//push a messages every seconds for 10 second then stop
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		log.Debugf("Start pushing to \"%s\" ...", *topicFlag)
		for i := 0; i < *nrFlag; i++ {
			if *delaySecondsFlag > 0 {
				time.Sleep(time.Second * time.Duration(*delaySecondsFlag))
			}

			//push next test message
			test := testReq{ClientID: *topicFlag, TestNr: i}

			testJSON, err := json.Marshal(test)
			if err != nil {
				log.Errorf("Failed to encode %+v: %v", test, err)
				continue
			}

			err = broker.Publish(*topicFlag, mq.NewMessage(testJSON))
			if err != nil {
				log.Errorf("Failed to push %s: %v", string(testJSON), err)
				continue
			}

			log.Debugf("Pushed %+v encoded as %+v", test, string(testJSON))
		}
		log.Debugf("Stop pushing.")

		//in memory queue, with both publish and consume in the same process
		//we need to close the queue to say this is it, nothing more coming!
		broker.Close()
		wg.Done()
	}()

	log.Debugf("Waiting...")
	wg.Wait()
	log.Debugf("Stopped.")
} //main()

//my test req+res looks like this:
type testReq struct {
	ClientID string `json:"client-id"`
	TestNr   int    `json:"nr"`
}

type basicSubscriber struct {
}

func (subscriber basicSubscriber) Process(ctx mq.IContext, msg mq.IMessage) error {
	var tr testReq
	err := json.Unmarshal(msg.GetData(), &tr)
	if err != nil {
		return logger.Wrapf(err, "Context[%s]: Failed to decode %T", ctx.ID(), tr)
	}

	log.Debugf("Context[%s]: %T=%+v", ctx.ID(), tr, tr)
	time.Sleep(time.Second * 2)
	log.Debugf("Context[%s]: End processing", ctx.ID())
	return nil
}

// subscriberConfig := models.Config{
// 	Username:     "admin",
// 	Password:     "admin",
// 	RabbitMqHost: "localhost",
// 	VirtualHost:  "/",
// 	SubscriberConfig: &models.SubscriberConfig{
// 		QueueName:       "myTestQueue",
// 		ExchangeName:    "amq.topic",
// 		BindingType:     enums.Topic,
// 		RoutingKey:      "*",
// 		PrefetchCount:   2, //was 1
// 		StrictQueueName: true,
// 		Durable:         true,
// 		AutoDeleteQueue: false,
// 		RequeueOnNack:   true,
// 	},
// }

//TODO:
//- Test context time out, and redelivery
//- Limit concurrency with lots of messages pushed before starting - limit with NATS
//- Fail to process - demonstrate errors and how they are handled and deleted
//- Can Run with non-durable... in NATS?
//- Test different context timeout and ack strategies
//- Test consumption by different groups of consumers
//- Auditing
//- Test invalid messages e.g. decoding failed, and ensure restart does not pick them up again, and clear them out...
//- Handle and audit and count process error
