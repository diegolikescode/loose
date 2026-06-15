package broker

import (
	"fmt"
	"io"
	"time"

	"github.com/phuslu/log"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	topic string
	partition int
	conn *kafka.Conn
}

func (c *Consumer) ConsumeMessages() error {
	log.Info().Msg("start message consumption")
	batch := c.conn.ReadBatchWith(kafka.ReadBatchConfig{MinBytes: 10 * KB, MaxBytes: 1 * MB, MaxWait: 5 * time.Second})

	b := make([]byte, 10 * KB)
	var err error
	for {
		n, err := batch.Read(b)
		if err != nil {
			if err == io.EOF {
				log.Error().Msg("all messages processed, EOF reached")
				if err = batch.Close(); err != nil {
					log.Error().Msg(err.Error())
					return err
				}
				log.Info().Msg("batch closed, will follow normally")
				return io.EOF
			}

			log.Error().Msg(fmt.Sprintf("error when reading messages, err=%s\n", err.Error()))
			break
		}

		// m := DefaultMsg{}
		// err = json.Unmarshal(b[:n], &m)
		// if err != nil {
		// 	log.Error().Msg(fmt.Sprintf("error when parsing a message, err=%s msg=%v\n", err.Error(), string(b[:n])))
		// }
		// fmt.Printf("MSG PROCESSED! time=%s text=%s", m.Time, m.Text)

		log.Info().Msg(string(b[:n]))
	}

	if err = batch.Close(); err != nil {
		log.Error().Msg(err.Error())
		return err
	}

	return nil
}

func NewConsumer(t string, p int) *Consumer {
	consumer:=&Consumer{
		topic: t,
		partition: p,
	}
	conn, err := ConnectToTopic(t, p)
	if err != nil {
		panic(err.Error())
	}
	consumer.conn = conn

	return consumer
}
