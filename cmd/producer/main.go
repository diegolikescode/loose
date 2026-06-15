package main

import (
	"fmt"
	"time"

	"github.com/diegolikescode/kafka-ws/internal/broker"
	"github.com/phuslu/log"
)

func main() {
	// vals := 300 * broker.KB
	prod := broker.NewProducer("new-topic", 0)

	for n := range 10_000 {
		fmt.Println("counting,", n)
		// value, _:= json.Marshal(broker.DefaultMsg{Time: time.Now().String(), Text: string(vals)})
		value:= fmt.Sprintf("msg_number=%v msg=%v", n, time.Now().String())
		time.Sleep(2 * time.Second)
		err := prod.WriteJSONMessage(value)
		if err != nil {
			log.Error().Msg(err.Error())
		}
	}
}
