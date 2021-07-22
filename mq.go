package mq

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/stewelarend/logger"
)

var log = logger.New()

//IBroker is a message broker to publish/subscribe
// to messages in the selected message queuing implementation
type IBroker interface {
	Publish(topic string, message IMessage) error

	//Subscribe is a blocking call that returns when
	// the connection break or the limit is reached etc...
	Subscribe(topics []string, nrWorkerThreads int, processor IProcessor) error

	//close prevents further publishing and continue running
	//subscriptions until they terminate when no more messages,
	//after all subscriptions ended, the connection is closed
	//and the broker becomes unusable
	Close()
}

//IProcessor process messages from a subscription
type IProcessor interface {
	Process(IContext, IMessage) error
}

//IContextManager interface
type IContextManager interface {
	//New creates a new context, blocking until one becomes free when limited
	New() IContext

	//Wait until all contexts are free (terminated/expired)
	Wait()
}

//IContext ...
type IContext interface {
	ID() string
	StartTime() time.Time
	Process(IMessage)
	Close(error)
}

type context struct {
	id        string
	startTime time.Time
	doneChan  chan bool
}

func newContext(id string, doneChan chan bool) IContext {
	c := &context{
		id:        id,
		startTime: time.Now(),
		doneChan:  doneChan,
	}
	return c
}

func (c *context) ID() string {
	return c.id
}

func (c *context) StartTime() time.Time {
	return c.startTime
}

func (c *context) Process(msg IMessage) {
	log.Debugf("Context[%s] %s: Processing", c.id, c.startTime.Format("2006-01-02 15:04:05"))
	time.Sleep(time.Second * 2)
}

func (c *context) Close(err error) {
	if c.doneChan == nil {
		log.Errorf("Context[%s] already closed before err=%v", c.id, err)
	}
	if err == nil {
		log.Debugf("Context[%s]: End with success", c.id)
	} else {
		log.Debugf("Context[%s]: End with error: %c", c.id, err)
	}

	//write into done chan
	c.doneChan <- true
	c.doneChan = nil
}

//NewContextManager is the constructor
func NewContextManager(max int) IContextManager {
	cm := &ctxMgr{
		contextDoneChan: make(chan bool, 10), //processed fast, 10 allow some buffering
	}

	//process done chan
	go func() {
		for {
			<-cm.contextDoneChan
			cm.contextWaitGroup.Done()
			log.Debugf("A context terminated...")
		}
	}()

	return cm
}

//ctxMgr implements IContextManager
type ctxMgr struct {
	//context (todo: will move to context manager)
	contextMutex  sync.Mutex //lock when incrementing the id
	nextContextID int        //incrementing for each new context
	//freeContextsChan chan IContext //all free context are put here
	//usedContextsList *list.List //kept here while in use, ordered by last use, add to front, expire at back

	//wait group drops to zero when all context are free
	contextWaitGroup sync.WaitGroup

	//chan for context to notify when they terminate
	contextDoneChan chan bool
}

func (cm *ctxMgr) New() IContext {
	//todo: if limited, wait...
	cm.contextMutex.Lock()
	defer cm.contextMutex.Unlock()

	ctx := newContext(fmt.Sprintf("%d", cm.nextContextID), cm.contextDoneChan)
	cm.nextContextID++

	//add to wait group
	//IContext.Close() must call wg.Done() to undo this
	cm.contextWaitGroup.Add(1)

	log.Debugf("Context[%s] CREATED", ctx.ID())
	return ctx
}

func (cm *ctxMgr) Wait() {
	log.Debugf("Waiting for contexts to terminate...")
	cm.contextWaitGroup.Wait()
	return
}

const topicPattern = `[a-zA-Z0-9]([a-zA-Z0-9_-]*[a-zA-Z0-9])*`

var topicRegex = regexp.MustCompile("^" + topicPattern + "$")

func Topics(csv string) ([]string, error) {
	topics := strings.Split(csv, ",")
	for i, topic := range topics {
		topic := strings.TrimSpace(topic)
		if !topicRegex.MatchString(topic) {
			return topics, logger.Wrapf(nil, "invalid topic=\"%s\"", topic)
		}
		topics[i] = topic
	}
	return topics, nil
}
