package main

import (
	"context"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func ConsumeMessages(ctx context.Context, settings *kafka.ConfigMap, topic string, serializer *Serializer[Person]) {
	consumer, err := kafka.NewConsumer(settings)

	if err != nil {
		log.Fatalf("Consumer: Невозможно создать консьюмера: %s\n", err)
	}

	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Consumer: Ошибка при подписке консьюмера: %s\n", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			ev, err := consumer.ReadMessage(10 * time.Second)
			if err != nil {
				if !err.(kafka.Error).IsTimeout() {
					log.Printf("Consumer: Ошибка получения сообщения: %s\n", err)
				}
				continue
			}

			person, err := serializer.Deserialize(ev.Value)
			if err != nil {
				log.Printf("Consumer: Ошибка десериализации: %s\n", err)
				continue
			} else {
				log.Printf("Consumer: Получено сообщение %s:\n\t%+v\n", ev.TopicPartition, person)
			}

			_, err = consumer.CommitMessage(ev)
			if err != nil {
				log.Printf("Consumer: Ошибка коммита: %s\n", err)
			}
		}
	}
}
