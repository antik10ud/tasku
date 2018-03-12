package tasku

import (
	"testing"
	"time"
	"log"
	"fmt"
)

func TestFoo(t *testing.T) {

	qs, err := NewQueueService(
		&Opts{
			Addresses: []string{"localhost:28015"},
			Db:        "queues",
		})

	if err != nil {
		log.Fatalln(err)
	}

	defer qs.Stop()

	qs.ResetToFactory()
	qs.Create("test")

	planner, err := qs.Receive("test", ReceiveOpts{Parallel: 1})
	if err != nil {
		log.Fatalln(err)
	}

	go func() {
		j := 0
		start := time.Now()
		for {

			select {
			case m := <-planner.C:
				qs.Delete("test", m.Id)
				j++
				if j%100 == 0 {
					println(fmt.Sprintf("recv: %d %fxSec", j, float64(j)/time.Now().Sub(start).Seconds()))
				}
				break
			}
		}
	}()
	go func() {
		i := 0
		start := time.Now()
		for {
			i++
			qs.Send("test", Message{
				Id:      qs.NewId(),
				Payload: "Payload",
			})
			if i%100 == 0 {
				println(fmt.Sprintf("send: %d %fxSec", i, float64(i)/time.Now().Sub(start).Seconds()))
			}
		}
	}()

	time.Sleep(30 * time.Second)

}
