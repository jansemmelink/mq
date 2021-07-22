package rabbitmq

import (
	"encoding/json"
	"sync"

	"github.com/jansemmelink/mq"
	"github.com/stewelarend/logger"
	"github.com/streadway/amqp"
)

var log = logger.New().WithLevel(logger.LevelDebug)

type Config struct {
	Server string `json:"server" doc:"Defaults to \"amqp://guest:guest@localhost:5672/\""`
}

func (c *Config) Validate() error {
	if c.Server == "" {
		c.Server = "amqp://guest:guest@localhost:5672/"
	}
	return nil
}

func New(c Config) (mq.IBroker, error) {
	if err := c.Validate(); err != nil {
		return nil, logger.Wrapf(err, "invalid config: %v", err)
	}
	b := &broker{
		config: c,
		queue:  map[string]amqp.Queue{},
	}

	var err error
	if b.conn, err = amqp.Dial(c.Server); err != nil {
		return nil, logger.Wrapf(err, "Failed to connect to RabbitMQ %s", c.Server)
	}

	//create a channel, which is where most of the API for getting things done resides:
	if b.ch, err = b.conn.Channel(); err != nil {
		b.conn.Close()
		return nil, logger.Wrapf(err, "Failed to open a channel")
	}

	return b, nil
}

type broker struct {
	config Config
	conn   *amqp.Connection
	ch     *amqp.Channel
	queue  map[string]amqp.Queue
}

func (b *broker) q(topic string) (amqp.Queue, error) {
	q, ok := b.queue[topic]
	if !ok {
		//first time for this topic:
		//to send, we must declare a queue for us to send to;
		//then we can publish a message to the queue:
		var err error
		q, err = b.ch.QueueDeclare(
			topic, // name
			false, // durable
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			return amqp.Queue{}, logger.Wrapf(err, "failed to declare a queue")
		}
		b.queue[topic] = q
	}
	return q, nil
}

func (b *broker) Publish(topic string, message mq.IMessage) error {
	q, err := b.q(topic)
	if err != nil {
		return logger.Wrapf(err, "noq")
	}

	jsonMessage, _ := json.Marshal(message)
	if err := b.ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         jsonMessage,
			DeliveryMode: 2, //persistent (0 or 1 are Transient) Persistent make messages survive a rabbit server restart
		}); err != nil {
		return logger.Wrapf(err, "failed to publish on %s", topic)
	}
	return nil
}

//Subscribe is a blocking call that returns when
// the connection break or the limit is reached etc...
func (b broker) Subscribe(topics []string, nrWorkerThreads int, processor mq.IProcessor) error {
	if len(topics) == 0 {
		return logger.Wrapf(nil, "no topics specified")
	}

	ctxMgr := mq.NewContextManager(1)

	//setting qos: this determines how many 'inflight' events are delivered on this channel before
	//they are acked...
	//Rabbit delivers round-robin to all consumers that have not yet reached their limit
	//so if we start one consumer doing only one thread and another with 5, then if the first is busy
	//work will go to the second until it is also busy. However, if first process quick enough and ack,
	//then it will get another event before the other worker gets to 5... each time a consumer acks
	//a task, it is put back in the round-robin list to get more events and it will keep getting events
	//until its has this nr of in-flight messages to work on...
	if nrWorkerThreads >= 1 {
		if err := b.ch.Qos(nrWorkerThreads, 0, false); err != nil {
			return logger.Wrapf(err, "Failed to set qos")
		}
	}

	//subscribe to all topics specified
	//each topic will have its own deliveryChannel
	//and the b.ch.Qos() called above ensures we only get N items in all those channels at a time
	//and that they are distributed evenly from all the subscriptions, not only the one with the most
	//entries
	topicRunning := sync.WaitGroup{}
	workerRunning := sync.WaitGroup{}
	for _, topic := range topics {
		q, err := b.q(topic)
		if err != nil {
			return logger.Wrapf(err, "noq")
		}
		deliveryChannel, err := b.ch.Consume(
			q.Name, // queue
			"",     // consumer
			false,  // auto-ack - TODO: make this false then ack manually after processing! If not acked, will re-queue and re-deliver
			false,  // exclusive - TODO: need this? not sure
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		if err != nil {
			return logger.Wrapf(err, "failed to consume topic=\"%s\"", topic)
		}

		//start consuming in the background so we can start in other topics as well
		topicRunning.Add(1)
		go func(topic string) {
			log.Debugf("Waiting for messages {topic:\"%s\"}. To exit press CTRL+C", q.Name)
			for d := range deliveryChannel {
				//start background worker
				workerRunning.Add(1)
				go func(d amqp.Delivery) {
					//log.Debugf("topic[%d]: Received a message: %s", topic, d.Body)
					if msg, err := mq.DecodeMessage(d.Body); err != nil {
						log.Errorf("failed to decode message: %s: %v", msg, err)
					} else {
						if err := processor.Process(ctxMgr.New(), msg); err != nil {
							log.Errorf("processing failed: %v", err)
							//note: message is still acked, otherwise this will block the queue
							//alternative, Process() could re-queue message, or we could indicate
							//how error should be dealt with, but I think it is safer to do that
							//as it is now inside Process() rather than here
						}
					}
					//processing done - ack the message to move to next
					d.Ack(false) //false to only ack this message, not all inflight messages
					workerRunning.Done()
				}(d)
			} //for events in this topic
			topicRunning.Done()
		}(topic)
	} //for each topic

	//wait for all topics to terminate
	topicRunning.Wait()

	//wait for all workers to complete
	workerRunning.Wait()
	return nil
}

//close prevents further publishing and continue running
//subscriptions until they terminate when no more messages,
//after all subscriptions ended, the connection is closed
//and the broker becomes unusable
func (b *broker) Close() {
	if b.ch != nil {
		b.ch.Close()
		b.ch = nil
	}
	if b.conn != nil {
		b.conn.Close()
		b.conn = nil
	}
}
