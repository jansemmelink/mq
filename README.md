# Introduction
MQ is a Message Queue Abstraction layer to build reliable message processing applications
with a Publish/Subscribe(=consume) interface that supports concurrent processing of messages.

Included are implementations of a simple local memory queue as well as clients for NATS Streaming and RabbitMQ.

# RabbitMQ
## Setup for RabbitMQ 
Configure RabbitMQ container in docker-compose.yml:
```
...
version: "3.2"
services:
    rabbitmq:
        image: rabbitmq:3-management-alpine
        container_name: 'rabbitmq'
        ports:
            - 5672:5672
            - 15672:15672
        volumes:
            - ~/.docker-conf/rabbitmq/data/:/var/lib/rabbitmq/
            - ~/.docker-conf/rabbitmq/log/:/var/log/rabbitmq
        networks:
            - rabbitmq_go_net

networks:
    rabbitmq_go_net:
        driver: bridge
```
Start local RabbitMQ with: ```docker compose up -d```
Open the admin console in a browser: http://localhost:15672
For a new installation, login with username ```guest``` and password ```guest``` which will have full access.
You can leave it like that, or create you own admin user with tag 'Administration' to have full access, then remove the administration tag from the guest user account. For examples below, you just need to monitor, so you can continue using guest account with/without admin rights.

## Examples for RabbitMQ
Two examples exist, one is called producer (publish messages) and one is called consumer (consumes the messages).
With RabbitMQ running (see setup above), you can start a consumer:
```
cd examples/consumer
go build
./consumer -d -mq rabbit
```
The consumer will do nothing if the queue is empty. Leave it running.
Now start the producer to publish 10 messages numbers 0,1,2,...,9:
```
cd ../../examples/producer
go build
./producer -d -mq rabbit
```
If you look now at the consumer, it will receive those messages in order, sleeping a while on each.
You can also look at the admin console to see how messages were published and consumed.

If consumers was not running, console will show messages in queue ramping up to 10 and as soon as consumer starts they will be consumed back to 0.

## Load-Balancing
To demonstrate load balancing:
1. STOP the consumer
2. Make sure the test queue is empty (web console: http://localhost:15672/#/queues)

### Balance: One Producer One Consumer
In this example, we produce before there are any consumers, then start a consumer to empty the queue.
Start by publishing 10 messages to default queue topic "test" with:
```
./producer -d -mq rabbit -n 10 -t test
```
Note: -d is for debug, -mq rabbit says to use RabbitMQ, -n 10 says 10 messages and -t test says topic=queue_name="test" (which are all defaults except debug mode and mq value :-)). So for each test below, always use -d and -mq rabbit along with other options specified, also for the consumer.

Now in admin console, the queue has 10 items to process: [Test Queue](http://localhost:15672/#/queues/%2F/test)
Now run the consumer in debug mode for this test topic:
```
./consumer -d -mq rabbit -t test
```
It will print all 10 messages numbers in sequence from 0,1,2,...,9 then be idle again. Afterwards the admin console will show the test queue is empty.
The consumer keep running indefinitely. You may stop it with ```<Ctrl><C>``` before the next section.
### Multiple Consumers
In this example, we run two consumers, then see how the load is balanced among them.
Create two terminals and run the consumer in each one with this command:
```
./consumer -d -mq rabbit -t test
```
Now produce 10 messages again (same as above) and not how they are delivered to both consumers round-robin, i.e. one of the consumers get message 0,2,4,6 and 8 while the other gets message 1,3,5,7 and 9.

### Multiple Topics
Often we want to separate events into different streams/topics to allow better control options.
In this example we publish events to two topics (test-1 and test-2) then run a consumer to consume from both to see in what order they are emptied.

In RabbitMQ admin console, create a [Queue] called "channel-orders" with these settings:
- Type: Classic
- Name: "channel-orders"
- Durability: Durable (to save messages)
- Auto Delete: no
- Arguments: None
- Click on [Add Queue]


In RabbitMQ admin console, create an [Exchange] called "v2-queue-order" which suggest the endpoint POST /v2/queue/order will send to this exchange.
Specify other details for the exchange:
- Type: topic (because it uses the topic to process the event routing)
- Durability: transient (because it just pass the message on)
- Auto Delete: yes (we do not store anything in the exchange)
- Internal: no (because messages are sent from producers into this exchange)
- Arguments: none
- Click on [Add Exchange]

Then in the list of exchanges, click on the new exchange called "v2-queue-order" and not it has not binding to a queue, so we need to specify that. Under "Add Binding" specified the following:
- [To Queue] "channel-orders"
- Routing Key: "client.*.order"
- Arguments: None
- Click on [Bind]

Now all messages sent to queue topic "client.*.order" will be routed to the single "client-orders" queue.
But they will be queued as received. So a burst of orders from one client followed by a few of another, will end up in the same queue in that order, so processing for the small client will have to wait for the big client to finish.



**TODO - not what we try to achieve!**



1. First make sure all consumers are stopped.
2. Produce topics for test-1 and test-2 with these commands:
```
./producer -d -mq rabbit -t test-1
./producer -d -mq rabbit -t test-2
```
Now the admin console shows these two queued each having 10 messages.
Now run a consumer that will consume both topics:
```
./consumer -d -mq rabbit -t "test-1,test-2"
```


### TODO
More examples needed to show:
- How one topic with lots of messages does not swamp other topic with few messages
    - run consumer on test-1 and test-2 with a 1second process delay
    - submit 20 to test-1 and then 5 to test-2 (before test-1 is empty
    - make sure both are consumed fairly
- subscribe on wildcard, e.g. test-*, so that consumer does not have to restart when new Q is created
- replay queue, not delete after processing - may need to produce events again from S3 or log list
- suspend a queue - if pick up error on processing on test-N but other test-M are fine, then only suspend test-N
- option to speed up test-N but slow on test-M (may not be required, but could be done in code, how?) may start consumer only for test-N? But want control over it... may not be required... just think how if and think if required...
- demonstrate API to write S3 + mq
- how will this be deployed? Likely rabbit in EC2, and do we need clustering or not? Persistent queue data is needed when EC2 restarts, and must be available from lambda api v2-queue
- speed test
