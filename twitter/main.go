package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	twitterKey         = ""
	twitterKeySecret   = ""
	twitterToken       = ""
	twitterTokenSecret = ""
)

var (
	// twitter
	api *twitter.Client

	//  kafka
	broker   = "localhost:9092"
	topic    = "tweets"
	maxRetry = 2
)

func init() {
	twitterConfig := oauth1.NewConfig(twitterKey, twitterKeySecret)
	token := oauth1.NewToken(twitterToken, twitterTokenSecret)
	httpClient := twitterConfig.Client(oauth1.NoContext, token)
	api = twitter.NewClient(httpClient)
}

func main() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	exitOnError(err)

	params := &twitter.StreamFilterParams{
		Track:         []string{"cybertruck"},
		StallWarnings: twitter.Bool(true),
	}
	stream, err := api.Streams.Filter(params)
	exitOnError(err)
	defer stream.Stop()

	sigChan := make(chan os.Signal, 1)
	defer close(sigChan)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("Listening to tweets. Quit with Ctrl-C")

loop:
	for {
		select {
		case event := <-stream.Messages:
			if tweet, ok := event.(*twitter.Tweet); ok {
				b, err := json.Marshal(tweet)
				exitOnError(err)
				exitOnError(publishMessage(producer, topic, b))
			}
		case <-sigChan:
			fmt.Println("\b\bsignal received, exiting...")
			break loop
		}
	}
}

func exitOnError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func publishMessage(producer *kafka.Producer, topic string, msg []byte) error {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: msg,
	}, deliveryChan)

	if err != nil {
		return err
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return fmt.Errorf("Delivery failed: %v", m.TopicPartition.Error)
	}

	fmt.Printf("Delivered message to topic %s [%d] at offset %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	return nil
}
