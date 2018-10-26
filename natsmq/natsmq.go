package natsmq

import (
	"encoding/json"
	"sync"

	"github.com/jansemmelink/mq"
	stan "github.com/nats-io/go-nats-streaming"
)

//broker implements mq.IBroker
type broker struct {
	config                Config
	mutex                 sync.Mutex
	stanOptions           []stan.Option
	stanConn              stan.Conn
	subscriptions         []*subscription
	subscriptionWaitGroup sync.WaitGroup
	closed                bool
}

//New creates the message broker
func New(c Config) mq.IBroker {
	b := &broker{
		config:        c,
		stanOptions:   make([]stan.Option, 0),
		stanConn:      nil,
		subscriptions: make([]*subscription, 0),
	}
	if c.URL != "" {
		b.stanOptions = append(b.stanOptions, stan.NatsURL(c.URL))
	}
	return b
}

func (b *broker) Publish(topic string, msg mq.IMessage) error {
	if b == nil {
		return log.Errorf(nil, "Called <nil>.Publish(%s)", topic)
	}
	if err := b.connect(); err != nil {
		return log.Errorf(err, "Cannot publish without connection")
	}
	jsonMessage, err := json.Marshal(msg)
	if err != nil {
		return log.Errorf(err, "Publish(%s): Failed to encode", topic)
	}
	if err := b.stanConn.Publish(topic, jsonMessage); err != nil {
		return log.Errorf(err, "Failed to Publish(%s)", topic)
	}
	return nil
}

//Subscribe implements IBroker.Subscribe()
//when the broker is closed, the subscription stops taking new messages and terminate
//   after processing the last messages it already took
func (b *broker) Subscribe(topic string, processor mq.IProcessor) error {
	if b == nil {
		return log.Errorf(nil, "Called <nil>.Subscribe(%s)", topic)
	}
	if err := b.connect(); err != nil {
		return log.Errorf(err, "Cannot subscribe without connection")
	}

	//create a new subscription
	s := &subscription{
		stanConn: b.stanConn,
		topic:    topic,
		done:     make(chan bool),
		ctxMgr:   mq.NewContextManager(1), //max concurrent
	}

	//prepare NATS subscription options
	var err error
	subscriptionOptions := make([]stan.SubscriptionOption, 0)
	subscriptionOptions = append(subscriptionOptions, stan.DurableName(topic))

	/*if all {
		log.Debug.Printf("Subscribing to start from 0...")
		subscriptionOptions = append(subscriptionOptions, stan.StartAtSequence(0))
	} else {
		log.Debug.Printf("NOT start from 0...")
	}*/

	//set ack timeout on this subscription
	//this is how long NATS allows us to ack/nack each message to indicate
	//if it should be requeued or not. If we don´t ack/nack within this time,
	//the message will be redelivered.
	//subscriptionOptions = append(subscriptionOptions, stan.AckWait(conf.MaxAckDur))

	//we do manual ack after processing a message when we know a new context is available
	//otherwise NATS will stream another message to us before we have context available
	//and we might wait long for a context, causing the message to timeout in NATS
	//subscriptionOptions = append(subscriptionOptions, stan.SetManualAckMode())

	//set the max in flight
	//subscriptionOptions = append(subscriptionOptions, stan.MaxInflight(conf.MaxConcurrent))
	//subscriptionOptions = append(subscriptionOptions, stan.AckWait(time.Second*500))

	//this is the blocking call and natsHandler will deal with concurrency
	//of message processing
	s.stanSubscription, err = b.stanConn.QueueSubscribe(
		topic, //topic
		b.config.ConsumerGroup,
		s.natsHandlerFunc(processor), //called for each message
		subscriptionOptions...)
	if err != nil {
		return log.Errorf(err, "Failed to subscribe to NATS subject=\"%s\"", topic)
	}

	log.Info.Printf("Subscribed to NATS subject=%s, group=%s(=subject)", topic, topic)

	b.mutex.Lock()
	b.subscriptionWaitGroup.Add(1)
	b.subscriptions = append(b.subscriptions, s)
	b.mutex.Unlock()

	//block until this subscription terminated
	<-s.done

	//terminated: tell b not to wait for this subscription
	//note: do not grab mutex here - broker.Close() may be holding it...
	log.Debug.Printf("Subscription(%s) terminated.", topic)
	b.subscriptionWaitGroup.Done()
	//todo: delete from array... ? not sure if good idea yet... delete(b.subscriptions, s)
	return nil
}

func (b *broker) Close() {
	//closing the broker does the following:
	// - prevent further publishing
	// - request all subscriptions to finish what they're doing then terminate
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if !b.closed {
		b.closed = true
		for _, s := range b.subscriptions {
			s.Close()
		}

		//todo: when last subscription closed - close the connection as well
		b.subscriptionWaitGroup.Wait()
	}
}

//connect if not already connected
func (b *broker) connect() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.stanConn == nil {
		var err error
		b.stanConn, err = stan.Connect(b.config.ClusterID, b.config.ClientID, b.stanOptions...)
		if err != nil {
			return log.Errorf(err,
				"Failed to connect to NATS server=\"%s\", cluster=\"%s\" as client=\"%s\"",
				b.config.URL, b.config.ClusterID, b.config.ClientID)
		}
		log.Info.Printf("Connected to NATS server \"%s\" cluster \"%s\" as client \"%s\"",
			b.config.URL, b.config.ClusterID, b.config.ClientID)
	}
	return nil
}

type subscription struct {
	stanConn         stan.Conn
	topic            string
	done             chan bool
	stanSubscription stan.Subscription
	closed           bool
	ctxMgr           mq.IContextManager
	//count usage
	msgCount  int
	deadCount int
}

func (s *subscription) natsHandlerFunc(processor mq.IProcessor) func(*stan.Msg) {
	return func(msg *stan.Msg) {
		s.natsHandler(msg, processor)
	}
}

func (s *subscription) natsHandler(natsMsg *stan.Msg, processor mq.IProcessor) {
	s.msgCount++

	//decode the serialized message into an IMessage
	msg, err := mq.DecodeMessage(natsMsg.Data)
	if err != nil {
		log.Error.Printf("Decoding failed: %v", err)
		s.stanConn.Publish("deadletter."+s.topic, natsMsg.Data)
		s.deadCount++
		log.Error.Printf("Pushed deadletter[%d]: %v", s.deadCount, err)
		return
	}

	//create a context - throttling happens in NATS streaming subscription, not here,
	//so here we create as many context as necessary
	//log.Debug.Printf("Decoded message[%d]: %+v", s.msgCount, decodedMessage)
	go func() {
		var err error
		ctx := s.ctxMgr.New()
		defer ctx.Close(err)
		err = processor.Process(
			ctx,
			mq.NewMessage(msg.GetData())) //todo specify more header into IMessage
	}()
	return
}

func (s *subscription) Close() {
	if !s.closed {
		s.stanSubscription.Close()
		s.closed = true
		log.Debug.Printf("Subscription(%s): CLOSED", s.topic)

		//wait for all concurrent processes to stop, currently only one
		s.ctxMgr.Wait()
		log.Debug.Printf("Subscription(%s): ALL Contexts terminated", s.topic)

		//tell subscription main to terminate
		s.done <- true
	}
}
