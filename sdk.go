package delay_queue

import (
	"time"

	"github.com/streadway/amqp"
)

// Publish publish delay msg with fireTime
func Publish(ch *amqp.Channel, delayQueueName string, msg amqp.Publishing, fireTime time.Time) error {
	msg.Headers["ts"] = fireTime.UnixNano()
	return ch.Publish("", delayQueueName, false, false, msg)
}

// PublishBytesMsg publish the bytes msg body with fireTime
func PublishBytesMsg(ch *amqp.Channel, delayQueueName string, msg []byte, fireTime time.Time) error {
	p := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         msg,
	}
	return Publish(ch, delayQueueName, p, fireTime)
}
