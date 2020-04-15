package delay_queue

import (
	"log"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/streadway/amqp"
	queue "github.com/zcong1993/delay-queue/pq"
)

type DelayQueue struct {
	conn        *rabbitmq.Connection
	workerCh    *rabbitmq.Channel
	delayCh     *rabbitmq.Channel
	delayQueue  amqp.Queue
	workerQueue amqp.Queue
	//exchangeName string
	workerQueueName string
	delayQueueName  string

	pq  *queue.Pq
	Log *logrus.Logger
}

type Stats struct {
	PqSize int
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func NewDelayQueueFromConn(conn *rabbitmq.Connection, workerQueueName, delayQueueName string) *DelayQueue {
	delayCh, err := conn.Channel()
	failOnError(err, "Failed to open delay channel")
	err = delayCh.Qos(10000, 0, false)
	failOnError(err, "Failed to set qos")
	delayQ, err := delayCh.QueueDeclare(
		delayQueueName, // name
		true,           // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	failOnError(err, "Failed to declare delay queue")

	workerCh, err := conn.Channel()
	failOnError(err, "Failed to open worker channel")
	workerQ, err := delayCh.QueueDeclare(
		workerQueueName, // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare worker queue")

	dq := &DelayQueue{
		conn:            conn,
		workerCh:        workerCh,
		delayCh:         delayCh,
		delayQueue:      delayQ,
		workerQueue:     workerQ,
		workerQueueName: workerQueueName,
		delayQueueName:  delayQueueName,
		pq:              queue.NewPq(),
		Log:             logrus.New(),
	}

	dq.start()
	return dq
}

func NewDelayQueueFromUri(connectUri string, workerQueueName, delayQueueName string) *DelayQueue {
	conn, err := rabbitmq.Dial(connectUri)
	failOnError(err, "Failed to open connection")
	return NewDelayQueueFromConn(conn, workerQueueName, delayQueueName)
}

func (dq *DelayQueue) start() {
	msgs, err := dq.delayCh.Consume(
		dq.delayQueueName, // queue
		"",                // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			ts, ok := d.Headers["ts"].(int64)
			if ok {
				if ts <= time.Now().UnixNano() {
					d := d
					go func() {
						dq.Log.Debugf("push to worker %s", d.Body)
						//publish to worker
						err := dq.workerCh.Publish(
							"",
							dq.workerQueue.Name,
							false,
							false,
							amqp.Publishing{
								Headers:      d.Headers,
								DeliveryMode: amqp.Persistent,
								ContentType:  "text/plain",
								Body:         d.Body,
							},
						)
						if err != nil {
							dq.Log.Error("push to worker error ", err)
						}
						err = d.Ack(false)
						if err != nil {
							dq.Log.Error("ack delay msg error ", err)
						}
						dq.Log.Debugf("push to worker %s ack err %+v", d.Body, err)
					}()
				} else {
					dq.Log.Debug("push to pq ", ts)
					dq.pq.Push(queue.NewItem(d, ts))
				}
			}
		}
	}()

	go func() {
		for {
			if dq.pq.Peek() != nil && dq.pq.Peek().Priority() <= time.Now().UnixNano() {
				it := dq.pq.Pop()
				go func() {
					dq.Log.Debugf("push to worker %s", it.Value().(amqp.Delivery).Body)
					d := it.Value().(amqp.Delivery)
					err := dq.workerCh.Publish(
						"",
						dq.workerQueue.Name,
						false,
						false,
						amqp.Publishing{
							Headers:      d.Headers,
							DeliveryMode: amqp.Persistent,
							ContentType:  "text/plain",
							Body:         d.Body,
						},
					)
					if err != nil {
						dq.Log.Error("push to worker error ", err)
					}
					err = d.Ack(false)
					if err != nil {
						dq.Log.Error("ack delay msg error ", err)
					}
				}()
			}
		}
	}()
}

func (dq *DelayQueue) Publish(msg amqp.Publishing, fireTime time.Time) error {
	msg.Headers["ts"] = fireTime.UnixNano()
	return dq.delayCh.Publish("", dq.delayQueue.Name, false, false, msg)
}

func (dq *DelayQueue) PublishBytesMsg(msg []byte, fireTime time.Time) error {
	p := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         msg,
	}
	return dq.Publish(p, fireTime)
}

func (dq *DelayQueue) GetDelayCh() *rabbitmq.Channel {
	return dq.delayCh
}

func (dq *DelayQueue) Close() {
	err := dq.delayCh.Close()
	if err != nil {
		dq.Log.Error("close delay ch error ", err)
	}
	err = dq.workerCh.Close()
	if err != nil {
		dq.Log.Error("close worker ch error ", err)
	}
	err = dq.conn.Close()
	if err != nil {
		dq.Log.Error("close conn error ", err)
	}
}

func (dq *DelayQueue) Stat() Stats {
	return Stats{PqSize: dq.pq.Size()}
}
