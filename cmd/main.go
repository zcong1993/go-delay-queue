package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	delay_queue "github.com/zcong1993/delay-queue"
)

func mustGetEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("env %s is required", key)
	}
	return val
}

func main() {
	rabbitUrl := mustGetEnv("RABBITMQ_URL")
	workerQueueName := mustGetEnv("WORKER_QUEUE_NAME")
	delayQueueName := mustGetEnv("DELAY_QUEUE_NAME")
	debug := os.Getenv("DEBUG_DELAY_QUEUE")

	idleConnsClosed := make(chan struct{})
	dq := delay_queue.NewDelayQueueFromUri(rabbitUrl, workerQueueName, delayQueueName)
	dq.Log.Infof("running, workerQueueName: %s, delayQueueName: %s", workerQueueName, delayQueueName)
	if debug == "true" {
		dq.Log.SetLevel(logrus.DebugLevel)
	}
	go func() {
		sigint := make(chan os.Signal, 1)

		// interrupt signal sent from terminal
		signal.Notify(sigint, os.Interrupt)
		// sigterm signal sent from kubernetes
		signal.Notify(sigint, syscall.SIGTERM)

		<-sigint
		dq.Log.Info("receive SIGTERM, will close")
		dq.Close()
		dq.Log.Info("exit")
		close(idleConnsClosed)
	}()

	<-idleConnsClosed
}
