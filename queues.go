package tasku

import (
	"time"
)

type Queue string

func (b Queue) String() string {
	return string(b)
}

type Payload interface{}

type Message struct {
	Id            string
	Payload       Payload
	NextDelivery  time.Time
	DeliveryError string
	DeliveryCount int
}

func NewQueueService(config *Opts) (*Service, error) {
	qs := new(Service)
	err := qs.init(config)
	if err != nil {
		return nil, err
	}

	return qs, nil
}
