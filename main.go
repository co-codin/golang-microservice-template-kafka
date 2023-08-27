package main

import (
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id": "test-client",
		"acks": "all",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	delivery_chan := make(chan kafka.Event, 10000)
	topic := "test-topic"

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value: []byte("test123")},
		delivery_chan,
	)

	if err != nil {
		log.Fatal(err)
	}

	e := <-delivery_chan
	fmt.Printf("%+v\n", e.String())

	fmt.Printf("%+v\n", p)
}