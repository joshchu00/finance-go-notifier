package main

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/config"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	"github.com/joshchu00/finance-go-notifier/twse"
	protobuf "github.com/joshchu00/finance-protobuf/inside"
)

func init() {

	// config
	config.Init()

	// logger
	logger.Init(config.LogDirectory(), "notifier")

	// log config
	logger.Info(fmt.Sprintf("%s: %s", "EnvironmentName", config.EnvironmentName()))
	logger.Info(fmt.Sprintf("%s: %s", "LogDirectory", config.LogDirectory()))
	logger.Info(fmt.Sprintf("%s: %s", "CassandraHosts", config.CassandraHosts()))
	logger.Info(fmt.Sprintf("%s: %s", "CassandraKeyspace", config.CassandraKeyspace()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaBootstrapServers", config.KafkaBootstrapServers()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaNotifierTopic", config.KafkaNotifierTopic()))
	logger.Info(fmt.Sprintf("%s: %s", "TelegramToken", config.TelegramToken()))
	logger.Info(fmt.Sprintf("%s: %s", "TelegramChatID", config.TelegramChatID()))
	logger.Info(fmt.Sprintf("%s: %s", "TelegramURLSendMessage", config.TelegramURLSendMessage()))
	logger.Info(fmt.Sprintf("%s: %s", "TelegramText", config.TelegramText()))
}

var environmentName string

func process() {

	if environmentName == config.EnvironmentNameProd {
		defer func() {
			if err := recover(); err != nil {
				logger.Panic(fmt.Sprintf("recover %v", err))
			}
		}()
	}

	var err error

	// cassandra client
	var cassandraClient *cassandra.Client
	cassandraClient, err = cassandra.NewClient(config.CassandraHosts(), config.CassandraKeyspace())
	if err != nil {
		logger.Panic(fmt.Sprintf("cassandra.NewClient %v", err))
	}
	defer cassandraClient.Close()

	// notifier consumer
	var notifierConsumer *kafka.Consumer
	notifierConsumer, err = kafka.NewConsumer(config.KafkaBootstrapServers(), "notifier", config.KafkaNotifierTopic())
	if err != nil {
		logger.Panic(fmt.Sprintf("kafka.NewConsumer %v", err))
	}
	defer notifierConsumer.Close()

	for {

		message := &protobuf.Notifier{}

		var topic string
		var partition int32
		var offset int64
		var value []byte

		topic, partition, offset, value, err = notifierConsumer.Consume()
		if err != nil {
			logger.Panic(fmt.Sprintf("Consume %v", err))
		}

		err = proto.Unmarshal(value, message)
		if err != nil {
			logger.Panic(fmt.Sprintf("proto.Unmarshal %v", err))
		}

		switch message.Exchange {
		case "TWSE":
			err = twse.Process(
				message.Symbol,
				message.Period,
				message.Datetime,
				message.Strategy,
				config.TelegramToken(),
				config.TelegramChatID(),
				config.TelegramURLSendMessage(),
				config.TelegramText(),
				cassandraClient,
			)
			if err != nil {
				logger.Panic(fmt.Sprintf("Process %v", err))
			}
		default:
			logger.Panic("Unknown exchange")
		}

		// strange
		offset++

		err = notifierConsumer.CommitOffset(topic, partition, offset)
		if err != nil {
			logger.Panic(fmt.Sprintf("CommitOffset %v", err))
		}
	}
}

func main() {

	logger.Info("Starting notifier...")

	// environment name
	switch environmentName = config.EnvironmentName(); environmentName {
	case config.EnvironmentNameDev, config.EnvironmentNameTest, config.EnvironmentNameStg, config.EnvironmentNameProd:
	default:
		logger.Panic("Unknown environment name")
	}

	for {

		process()

		time.Sleep(3 * time.Second)

		if environmentName != config.EnvironmentNameProd {
			break
		}
	}
}
