package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func ProduceMessages(settings *kafka.ConfigMap, topic string, messages []Person, serializer *Serializer[Person]) {
	p, err := kafka.NewProducer(settings)
	if err != nil {
		log.Fatalf("Producer: Ошибка при создании продьюсера: %v\n", err)
	}
	defer p.Close()

	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)
	for _, message := range messages {
		bytes := serializer.Serialize(message)

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(message.Name),
			Value: bytes,
		}, deliveryChan)

		if err != nil {
			log.Printf("Producer: Ошибка при отправке сообщения: %v\n", err)
			continue
		}

		event := <-deliveryChan
		msg := event.(*kafka.Message)

		if msg.TopicPartition.Error != nil {
			log.Printf("Producer: Ошибка доставки сообщения: %v\n", msg.TopicPartition.Error)
		} else {
			log.Printf("Producer: Сообщение отправлено в %s:\n\t%+v\n",
				msg.TopicPartition,
				message)
		}
	}
}
