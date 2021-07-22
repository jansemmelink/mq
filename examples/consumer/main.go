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
	topicsFlag := flag.String("t", "test", "Topics must consist only of [a-zA-Z0-9_-] and use CSV for multiple topics")
	nrWorkersFlag := flag.Int("w", 1, "Nr of worker threads")
	delaySecondsFlag := flag.Int("T", 0, "delay in seconds to process")
	debugFlag := flag.Bool("d", false, "Run with DEBUG output")
	mqFlag := flag.String("mq", "mem", "Type of mq: mem|nats|rabbit")
	flag.Parse()
	if *debugFlag {
		log = log.WithLevel(logger.LevelDebug)
	}
	if *nrWorkersFlag < 1 {
		panic(fmt.Errorf("-w %d must be >= 1", *nrWorkersFlag))
	}
	topics, err := mq.Topics(*topicsFlag)
	if err != nil {
		panic(logger.Wrapf(err, "invalid topics"))
	}

	// topics, err := rabbitmq.Topics(*topicFlag)
	// if err != nil {
	// 	panic(fmt.Errorf("Invalid topics: %v", err))
	// }

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

	log.Debugf("Subscribing to topics:[%v] ...", topics)

	//this is a simple subscription that does a a blocking call that runs forever:
	if err := broker.Subscribe(
		topics,
		*nrWorkersFlag,
		basicSubscriber{
			processSleep: time.Second * time.Duration(*delaySecondsFlag),
		},
	); err != nil {
		panic(fmt.Errorf("failed to subscribe to topics[%v]: %v", topics, err))
	}
	log.Debugf("Subscription terminated.")
	broker.Close()
} //main()

//my test req+res looks like this:
type testReq struct {
	ClientID string `json:"client-id"`
	TestNr   int    `json:"nr"`
}

type basicSubscriber struct {
	processSleep time.Duration
}

var (
	nrWorkers   = 0
	maxWorkers  = 0
	workerMutex sync.Mutex
)

func workerCount(delta int) {
	workerMutex.Lock()
	defer workerMutex.Unlock()
	nrWorkers += delta
	if nrWorkers > maxWorkers {
		maxWorkers = nrWorkers
		log.Debugf("MAX WORKERS=%d", maxWorkers)
	}
}

func (subscriber basicSubscriber) Process(ctx mq.IContext, msg mq.IMessage) error {
	workerCount(1)
	defer workerCount(-1)
	var tr testReq
	err := json.Unmarshal(msg.GetData(), &tr)
	if err != nil {
		return logger.Wrapf(err, "Context[%s]: Failed to decode %T", ctx.ID(), tr)
	}

	log.Debugf("Context[%s]: %T=%+v", ctx.ID(), tr, tr)
	if subscriber.processSleep > 0 {
		time.Sleep(subscriber.processSleep)
		//log.Debugf("Context[%s]: End processing (=sleep %v)", ctx.ID(), subscriber.processSleep)
	}
	return nil
}
