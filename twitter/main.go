package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
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
	brokerList = []string{"localhost:9092"}
	topic      = "test-topic"
	maxRetry   = 2
)

func init() {
	twitterConfig := oauth1.NewConfig(twitterKey, twitterKeySecret)
	token := oauth1.NewToken(twitterToken, twitterTokenSecret)
	httpClient := twitterConfig.Client(oauth1.NoContext, token)
	api = twitter.NewClient(httpClient)

}

func main() {
	producer, err := initSarama()
	exitOnError(err)
	defer func() { exitOnError(producer.Close()) }()

	params := &twitter.StreamFilterParams{
		Track:         []string{"linux"},
		StallWarnings: twitter.Bool(true),
	}
	stream, err := api.Streams.Filter(params)
	exitOnError(err)
	defer stream.Stop()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("Listening to tweets. Quit with Ctrl-C")

loop:
	for {
		select {
		case event := <-stream.Messages:
			if tweet, ok := event.(*twitter.Tweet); ok {
				b, err := json.Marshal(tweet)
				exitOnError(err)
				publishMessage(producer, topic, b)
			}
		case <-signalChan:
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

func initSarama() (sarama.SyncProducer, error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Retry.Max = 2
	kafkaConfig.Producer.Return.Successes = true

	return sarama.NewSyncProducer(brokerList, kafkaConfig)
}

func publishMessage(producer sarama.SyncProducer, topic string, msg []byte) error {
	kafkaMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg),
	}

	partition, offset, err := producer.SendMessage(kafkaMessage)
	if err != nil {
		fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	}

	return err
}
