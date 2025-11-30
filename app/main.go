package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

type AppSettings struct {
	UseSasl           bool
	BootstrapServers  string
	CertificatePath   string
	TopicName         string
	SchemaRegistryUrl string
	ProducerUser      string
	ProducerPassword  string
	ConsumerUser      string
	ConsumerPassword  string
	ConsumerGroup     string
}

func main() {
	settings := getAppSettings()
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	msgs := []Person{
		{Name: "Victor", JobTitle: "Developer", Email: "Victor@mail.ru"},
		{Name: "Alex", JobTitle: "Manager", Email: "Alex@mail.ru"},
	}

	producerCfg := getProducerConfig(settings)
	WaitTopic(ctx, producerCfg, settings.TopicName)

	serConfig := getRegistryConfig(settings)

	WaitRegistry(ctx, serConfig)
	serializer := NewSerializer[Person](serConfig, settings.TopicName)

	var wg sync.WaitGroup
	wg.Go(func() {
		ProduceMessages(producerCfg, settings.TopicName, msgs, serializer)
	})
	wg.Go(func() {
		ConsumeMessages(ctx, getConsumerConfig(settings), settings.TopicName, serializer)
	})

	wg.Wait()
	log.Println("Завершено")
}

func getAppSettings() AppSettings {
	if os.Getenv("IS_YANDEX") == "true" {
		return AppSettings{
			UseSasl:           true,
			BootstrapServers:  "rc1a-r7oq7lotupna90g2.mdb.yandexcloud.net:9091,rc1b-4q4asfghkt4uei74.mdb.yandexcloud.net:9091,rc1d-eijh65c86cgfhcd9.mdb.yandexcloud.net:9091",
			CertificatePath:   "cert/YandexInternalRootCA.crt",
			TopicName:         "topic1",
			SchemaRegistryUrl: "https://rc1a-r7oq7lotupna90g2.mdb.yandexcloud.net:443/",
			ProducerUser:      "topic1_producer",
			ProducerPassword:  "***",
			ConsumerUser:      "topic1_consumer",
			ConsumerPassword:  "***",
			ConsumerGroup:     "app-consumer-group",
		}
	}

	return AppSettings{
		UseSasl:           false,
		BootstrapServers:  "kafka-broker:9094",
		CertificatePath:   "",
		TopicName:         "topic1",
		SchemaRegistryUrl: "http://schema-registry:8080/",
		ProducerUser:      "",
		ProducerPassword:  "",
		ConsumerUser:      "",
		ConsumerPassword:  "",
		ConsumerGroup:     "app-consumer-group",
	}
}

func getRegistryConfig(settings AppSettings) *schemaregistry.Config {
	if settings.UseSasl {
		serConfig := schemaregistry.NewConfigWithBasicAuthentication(
			settings.SchemaRegistryUrl,
			settings.ProducerUser,
			settings.ProducerPassword)
		serConfig.SslCaLocation = settings.CertificatePath
		return serConfig
	}

	return schemaregistry.NewConfig(settings.SchemaRegistryUrl)
}

func getProducerConfig(settings AppSettings) *kafka.ConfigMap {
	if settings.UseSasl {
		return &kafka.ConfigMap{
			"bootstrap.servers":        settings.BootstrapServers,
			"ssl.ca.location":          settings.CertificatePath,
			"security.protocol":        "SASL_SSL",
			"sasl.mechanism":           "SCRAM-SHA-512",
			"sasl.username":            settings.ProducerUser,
			"sasl.password":            settings.ProducerPassword,
			"message.timeout.ms":       10000,
			"message.send.max.retries": 3,
			"acks":                     "all",
		}
	}

	return &kafka.ConfigMap{
		"bootstrap.servers":        settings.BootstrapServers,
		"message.timeout.ms":       10000,
		"message.send.max.retries": 3,
		"acks":                     "all",
	}
}

func getConsumerConfig(settings AppSettings) *kafka.ConfigMap {
	if settings.UseSasl {
		return &kafka.ConfigMap{
			"bootstrap.servers":  settings.BootstrapServers,
			"ssl.ca.location":    settings.CertificatePath,
			"security.protocol":  "SASL_SSL",
			"sasl.mechanism":     "SCRAM-SHA-512",
			"sasl.username":      settings.ConsumerUser,
			"sasl.password":      settings.ConsumerPassword,
			"group.id":           settings.ConsumerGroup,
			"session.timeout.ms": 6000,
			"enable.auto.commit": false,
			"auto.offset.reset":  "earliest",
		}
	}

	return &kafka.ConfigMap{
		"bootstrap.servers":  settings.BootstrapServers,
		"group.id":           settings.ConsumerGroup,
		"session.timeout.ms": 6000,
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	}
}
