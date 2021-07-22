package memmq

import (
	"fmt"

	"github.com/jansemmelink/mq"
	"github.com/stewelarend/logger"
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

func (b *broker) Subscribe(topics []string, nrWorkerThreads int, processor mq.IProcessor) error {
	if len(topics) != 1 {
		return logger.Wrapf(nil, "NYI len(topics=%v)=%d != 1", topics, len(topics))
	}
	//topic := topics[0]
	if nrWorkerThreads != 1 {
		return fmt.Errorf("NYI nrWorkerThreads!=1")
	}
	//run for ever to process messages
	for {
		log.Debugf("Waiting for message...")
		msg, ok := <-b.messageChannel
		if !ok {
			log.Debugf("Broker closed. Subscription stopped.")
			return nil
		}
		log.Debugf("Processing message...")
		err := processor.Process(
			b.ctxMgr.New(),
			msg) //todo - will crash with nil context, but not yet doing concurrent processing
		if err != nil {
			log.Errorf("Failed to process message: %v", err)
		} else {
			log.Debugf("Processed successfully.")
		}
	}
	//return logger.Wrapf(nil, "Should not get here yet... should be running forever???")
}

func (b *broker) Close() {
	if !b.closed {
		close(b.messageChannel)
		b.closed = true
	}
	//when doing concurrent processing, need to wait for context to end
}
