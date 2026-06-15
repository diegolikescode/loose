package broker

import (
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	topic string
	partition int
	conn *kafka.Conn
}


func (p *Producer) WriteJSONMessage(msg any) error {
	val, err:= json.Marshal(msg)
	if err != nil {
		return err
	}

	_, err = p.conn.WriteMessages(kafka.Message{Value: val})
	if err != nil {
		return err
	}
	return nil
}

func NewProducer(t string, p int) *Producer {
	prod :=&Producer{
		topic: t,
		partition: p,
	}
	conn, err := ConnectToTopic(t, p)
	if err != nil {
		panic(err.Error())
	}
	prod.conn = conn
	return prod
}
