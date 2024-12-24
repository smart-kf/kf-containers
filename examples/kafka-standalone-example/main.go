package main

import (
	"context"
	"github.com/IBM/sarama"
	"log"
	"time"
)

const addr = `172.17.0.1:9092`

//const addr = `kafka:9092`

func main() {
	go consumer()
	// 创建 Kafka 生产者
	producer, err := sarama.NewSyncProducer([]string{addr}, nil)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	// 发送消息
	topic := "test-topic"

	tk := time.NewTicker(1 * time.Second)
	for range tk.C {
		message := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder("Hello, Kafka with Sarama!"),
		}
		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			log.Fatalf("Failed to send message: %s", err)
		}
		log.Printf("Message sent to partition %d with offset %d\n", partition, offset)
	}
}

func consumer() {
	// 创建 Kafka 消费者
	c := Consumer{
		ready: make(chan bool),
	}

	config := sarama.NewConfig()
	config.Version = sarama.DefaultVersion
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := sarama.NewConsumerGroup([]string{addr}, "test-group", config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	if err := client.Consume(ctx, []string{"test-topic"}, &c); err != nil {
		log.Fatal(err)
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}
			log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
