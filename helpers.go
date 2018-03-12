package tasku

import (
	"time"
	"log"
)

type RetryOpts struct {
	RetryPlan []time.Duration
}
type RetryPolicy interface {
	NextDelivery(service *Service, queue Queue, message *Message) time.Time
}

type UserReceiver interface {
	Receive(Queue, *Message) error
}

var DefaultReceiveOpts = ReceiveOpts{Parallel: 1, Hold: 60 * time.Minute}

var DefaultRetryOpts = RetryOpts{RetryPlan: []time.Duration{60 * time.Minute, 120 * time.Minute, 240 * time.Minute}}

var DropMessage = time.Time{}

func (strategy RetryOpts) NextDelivery(service *Service, queue Queue, message *Message) time.Time {
	l := len(strategy.RetryPlan)
	i := message.DeliveryCount - 1
	if i >= l {
		i = l - 1
	} else if i < 0 {
		i = 0
	}
	duration := strategy.RetryPlan[i]
	if duration == 0 {
		return DropMessage
	}
	return service.Now().UTC().Add(duration)
}

func WithRetryStrategy(service *Service, queue Queue, receiverOpts ReceiveOpts, retryPolicy RetryPolicy, userReceiver UserReceiver) (chan bool, error) {
	receiver, err := service.Receive(queue, receiverOpts)
	if err != nil {
		return nil, err
	}
	c := make(chan bool)
	go func() {
	loop:
		for {
			select {
			case m, ok := <-receiver.C:
				if !ok {
					break loop
				}
				//TODO:retry opts can be ovewritten by payload message
				m.NextDelivery = retryPolicy.NextDelivery(service, queue, m)
				log.Printf("msg %s/%s received", queue, m.Id)
				err := userReceiver.Receive(queue, m)
				if err == nil {
					err = service.Delete(queue, m.Id)
					if err != nil {
						log.Printf("msg %s/%s delete failed: %s", queue, m.Id, err)
					} else {
						log.Printf("msg %s/%s deleted", queue, m.Id)
					}
					break
				}
				if m.NextDelivery == DropMessage { // override drop when an error ocurred
					log.Printf("msg %s/%s next delivery is 'drop' but client return err %s", queue, m.Id, err)
					m.NextDelivery = service.Now().Add(60 * time.Minute) //TODO:to props
				}
				m.DeliveryError = err.Error()
				ok, updateErr := service.Update(queue, m)
				if updateErr != nil {
					log.Printf("msg %s/%s fail to update message due to %s, (message delivery error was %s)", queue, m.Id, updateErr, err)
				} else {
					if !ok {
						log.Printf("msg %s/%s was not found, maybe was deleted (message delivery error was %s)", queue, m.Id, err)
					} else {
						log.Printf("msg %s/%s next delivery is %s due to %s", queue, m.Id, m.NextDelivery.Format(time.RFC3339), err)
					}
				}
				break
			}
		}
		c <- true
	}()
	return c, nil
}
