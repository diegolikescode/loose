package broker

import (
	"context"
	"fmt"

	"github.com/phuslu/log"
	"github.com/segmentio/kafka-go"
)

type DefaultMsg struct {
	Time string
	Text string
}

const (
	_ = iota
	KB = 1 << (10 * iota)
	MB = 1 << (10 * iota)
	GB = 1 << (10 * iota)
)

func ConnectToTopic(topic string, partition int) (*kafka.Conn, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Info().Msg(fmt.Sprintf("error when connecting to kafka topic. err=%s\n", err.Error()))
		return nil, err
	}

	return conn, nil
}
