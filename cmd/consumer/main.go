package main

import (
	"fmt"
	"io"
	"time"

	"github.com/diegolikescode/kafka-ws/internal/broker"
	"github.com/phuslu/log"
)

func main() {
	consumer := broker.NewConsumer("new-topic", 0)

	for {
		log.Info().Msg("will start processing messages")
		err := consumer.ConsumeMessages()
		if err == io.EOF {
			time.Sleep(4 * time.Second)
		} else {
			log.Warn().Msg(fmt.Sprintf("Deu merda err=%s\n", err.Error()))
			break
		}
	}
}
