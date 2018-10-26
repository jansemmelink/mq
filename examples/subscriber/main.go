package main

import (
	"encoding/json"
	"sync"
	"time"

	"conor.co.za/vservices/golib/logger"
	"github.com/jansemmelink/flags"
	"github.com/jansemmelink/mq"
	"github.com/jansemmelink/mq/memmq"
	"github.com/jansemmelink/mq/natsmq"
)

var log = logger.New("mq-sub")

func main() {
	debugFlag := flags.Bool("-d", "--debug", false, "Run with DEBUG output")
	mqFlag := flags.String("", "--mq", "mem", "Type of mq: mem|nats|rabbit")
	flags.Parse()
	if debugFlag.Value().(bool) {
		logger.SetDefaultLevel(logger.LevelDebug)
	}

	var broker mq.IBroker
	switch mqFlag.Value().(string) {
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
		panic("NYI")
	default:
		flags.Usage("Invalid --mq=" + mqFlag.Value().(string))
		return
	}
	defer broker.Close()

	//push a messages every seconds for 10 second then stop
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		log.Debug.Printf("Start pushing...")
		for i := 0; i < 10; i++ {
			time.Sleep(time.Second * 1)

			//push next test message
			test := testReq{TestNr: i}

			testJSON, err := json.Marshal(test)
			if err != nil {
				log.Error.Printf("Failed to encode %+v: %v", test, err)
				continue
			}

			err = broker.Publish("test", mq.NewMessage(testJSON))
			if err != nil {
				log.Error.Printf("Failed to push %s: %v", string(testJSON), err)
				continue
			}

			log.Debug.Printf("Pushed %+v encoded as %+v", test, string(testJSON))
		}
		log.Debug.Printf("Stop pushing.")

		//in memory queue, with both publish and consume in the same process
		//we need to close the queue to say this is it, nothing more coming!
		broker.Close()
		wg.Done()
	}()

	//subscribe is a blocking call that runs forever:
	broker.Subscribe("test", basicSubscriber{})
	log.Debug.Printf("Subscription terminated.")

	log.Debug.Printf("Waiting...")
	wg.Wait()
	log.Debug.Printf("Stopped.")
} //main()

//my test req+res looks like this:
type testReq struct {
	TestNr int `json:"nr"`
}

type basicSubscriber struct {
}

func (subscriber basicSubscriber) Process(ctx mq.IContext, msg mq.IMessage) error {
	var tr testReq
	err := json.Unmarshal(msg.GetData(), &tr)
	if err != nil {
		return log.Errorf(err, "Context[%s]: Failed to decode %T", ctx.ID(), tr)
	}

	log.Debug.Printf("Context[%s]: %T=%+v", ctx.ID(), tr, tr)
	time.Sleep(time.Second * 2)
	log.Debug.Printf("Context[%s]: End processing", ctx.ID())
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
