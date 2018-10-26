package memmq

import (
	"github.com/jansemmelink/mq"
)

//broker implements mq.IBroker
type broker struct {
	config         Config
	messageChannel chan mq.IMessage
	closed         bool
	ctxMgr         mq.IContextManager
}

//New creates the message broker
func New(c Config) mq.IBroker {
	b := &broker{
		config:         c,
		messageChannel: make(chan mq.IMessage, 100),
		ctxMgr:         mq.NewContextManager(1),
	}
	return b
}

func (b *broker) Publish(topic string, msg mq.IMessage) error {
	b.messageChannel <- msg
	return nil
}

func (b *broker) Subscribe(topic string, processor mq.IProcessor) error {
	//run for ever to process messages
	for {
		log.Debug.Printf("Waiting for message...")
		msg, ok := <-b.messageChannel
		if !ok {
			log.Debug.Printf("Broker closed. Subscription stopped.")
			return nil
		}
		log.Debug.Printf("Processing message...")
		err := processor.Process(
			b.ctxMgr.New(),
			msg) //todo - will crash with nil context, but not yet doing concurrent processing
		if err != nil {
			log.Error.Printf("Failed to process message: %v", err)
		} else {
			log.Debug.Printf("Processed successfully.")
		}
	}
	//return log.Errorf(nil, "Should not get here yet... should be running forever???")
}

func (b *broker) Close() {
	if !b.closed {
		close(b.messageChannel)
		b.closed = true
	}
	//when doing concurrent processing, need to wait for context to end
}
