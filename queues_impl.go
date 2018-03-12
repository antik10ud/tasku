package tasku

import (
	"time"
	db "gopkg.in/gorethink/gorethink.v3"
	"errors"
	"gopkg.in/gorethink/gorethink.v3/encoding"

	"log"
	"fmt"
	"math/rand"

	"github.com/antik10ud/go-uids/uid16"
)

type dbMessage struct {
	Id            string      `json:"id,omitempty"`
	Payload       interface{} `json:"payload,omitempty"`
	NextDelivery  time.Time   `json:"next_delivery,omitempty"`
	DeliveryCount int         `json:"delivery_count,omitempty"`
}

type Service struct {
	session *db.Session
	dbname  string
}

type Opts struct {
	Addresses []string
	Db        string
}

var (
	bigBangDate  = time.Date(1970, 0, 0, 0, 0, 0, 0, time.UTC)
	endWorldDate = time.Date(2111, 11, 11, 11, 11, 11, 11, time.UTC)
	uidGen       = uid16.NewFactory()
)

func (service *Service) init(config *Opts) error {
	var err error
	db.SetTags("gorethink", "json")
	service.session, err = connectDB(config)
	if err != nil {
		return err
	}
	if len(config.Db) == 0 {
		return errors.New("missing queues db name")
	}
	service.dbname = config.Db

	return nil
}

func connectDB(config *Opts) (*db.Session, error) {
	session, err := db.Connect(db.ConnectOpts{
		Addresses:  config.Addresses,
		InitialCap: 10,
		MaxOpen:    10,
	})
	if err != nil {
		return nil, err
	}
	return session, nil
}

func (service *Service) ResetToFactory() error {
	err, exists := existsDB(service.session, service.dbname)
	if err != nil {
		return err
	}
	if exists {
		_, err = db.DBDrop(service.dbname).RunWrite(service.session)
		if err != nil {
			return err
		}
	}

	_, err = db.DBCreate(service.dbname).RunWrite(service.session)
	if err != nil {
		return err
	}

	/*
	TODO:db do not exists so how can we wait for it (lib throw error)
	_, err = db.DB(service.dbname).Wait().RunWrite(service.session)
	if err != nil {
		return err
	}*/

	return nil

}

func (service *Service) tableName(name Queue) string {
	return name.String()
}

func (service *Service) Create(queue Queue) error {
	name := service.tableName(queue)
	err, exists := existTable(service.session, service.dbname, name)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	_, err = db.DB(service.dbname).TableCreate(name).RunWrite(service.session)
	if err != nil {
		return err
	}

	_, err = db.DB(service.dbname).Table(name).Wait().RunWrite(service.session)
	if err != nil {
		return err
	}

	_, err = db.DB(service.dbname).Table(name).IndexCreate("next_delivery").RunWrite(service.session)
	if err != nil {
		return err
	}

	_, err = db.DB(service.dbname).Table(name).IndexWait("next_delivery").RunWrite(service.session)
	if err != nil {
		return err
	}
	return nil
}

func (service *Service) Drop(queue Queue) error {
	name := service.tableName(queue)
	err, exists := existTable(service.session, service.dbname, name)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	_, err = db.DB(service.dbname).TableDrop(name).RunWrite(service.session)
	if err != nil {
		return err
	}
	_, err = db.DB(service.dbname).Table(name).Wait().RunWrite(service.session)
	if err != nil {
		return err
	}
	return nil
}

func (service *Service) Delete(queue Queue, messageId string) error {
	table := service.tableName(queue)
	response, err := db.DB(service.dbname).Table(table).Get(messageId).Delete(db.DeleteOpts{Durability: "soft", ReturnChanges: false}).RunWrite(service.session)
	if err != nil {
		return err
	}
	if response.Errors != 0 {
		return errors.New(response.FirstError)
	}
	return nil
}

func (service *Service) Send(queue Queue, message Message) error {
	dbm := dbMessage{
		Id:            message.Id,
		Payload:       message.Payload,
		NextDelivery:  message.NextDelivery,
		DeliveryCount: message.DeliveryCount,
	}
	if dbm.NextDelivery == (time.Time{}) {
		dbm.NextDelivery = service.Now()
	}
	table := service.tableName(queue)
	response, err := db.DB(service.dbname).Table(table).Insert(dbm, db.InsertOpts{Durability: "hard", ReturnChanges: false, Conflict: "error"}).RunWrite(service.session)
	if err != nil {
		return err
	}
	if response.Errors != 0 {
		return errors.New(response.FirstError)
	}
	return nil
}

func (service *Service) Update(queue Queue, message *Message) (bool, error) {
	dbm := &dbMessage{
		Id:            message.Id,
		Payload:       message.Payload,
		NextDelivery:  message.NextDelivery,
		DeliveryCount: message.DeliveryCount,
	}
	table := service.tableName(queue)
	response, err := db.DB(service.dbname).Table(table).Get(dbm.Id).Update(dbm, db.UpdateOpts{Durability: "soft", ReturnChanges: false}).RunWrite(service.session)
	if err != nil {
		return false, err
	}

	if response.Errors != 0 {
		return false, errors.New(response.FirstError)
	}

	if response.Replaced <= 0 {
		return false, nil
	}

	return true, nil
}

func (service *Service) Stop() error {
	//service. -- shutdown consumers?
	err := service.session.Close(db.CloseOpts{NoReplyWait: false})
	return err
}

func (service *Service) Now() time.Time {
	return time.Now()
}

func (service *Service) Receive(queue Queue, recvOpts ReceiveOpts) (*Receiver, error) {
	receiver := Receiver{}
	err := receiver.init(service, queue, recvOpts)
	if err != nil {
		return nil, err
	}

	return &receiver, nil
}

func (service *Service) NewId() string {
	return uidGen.New().String()
}

type Receiver struct {
	hold    time.Duration
	queue   Queue
	service *Service
	C       chan *Message
}

type ReceiveOpts struct {
	Parallel int
	Hold     time.Duration
}

func (b ReceiveOpts) String() string {
	return fmt.Sprintf("parallel %d hold %s", b.Parallel, b.Hold)
}

func (recv *Receiver) init(service *Service, queue Queue, recvOpts ReceiveOpts) error {
	recv.queue = queue
	recv.service = service
	recv.hold = recvOpts.Hold
	parallel := recvOpts.Parallel
	if parallel == 0 {
		parallel = 10
	}
	recv.C = make(chan *Message, parallel)
	if recv.hold == 0 {
		recv.hold = time.Second * 60
	}

	log.Printf("start receive loop %s with parallel %d hold %s", queue, parallel, recv.hold)
	err := recv.receiveLoop()
	if err != nil {
		return err
	}

	return nil
}

func (recv *Receiver) receiveLoop() error {
	go func() {
		for {
			for {
				c := recv.C
				cap := cap(c) - len(c)
				if cap > 0 {
					n := recv.pickMessages(cap)
					if n == 0 {
						break
					}
				} else {
					log.Printf("warn undercap %d for %s", len(c), recv.queue)
					time.Sleep(3 * time.Second) //TODO:to const default no messges sleep
				}
			}
			time.Sleep(time.Duration(rand.Int63n(1000)+1000) * time.Millisecond) //TODO:to const default no messges sleep
		}
	}()
	return nil
}

func (recv *Receiver) pickMessages(n int) int {

	msgs, err := recv.service.pick(recv.queue, recv.hold, n)
	if err != nil {
		log.Printf("fail to pick msgs from %s: %s", recv.queue, err)
		return 0
	}
	l := len(msgs)
	for _, m := range msgs {
		recv.C <- m
	}
	return l
}

func (service *Service) pick(queue Queue, hold time.Duration, limit int) ([]*Message, error) {
	table := service.tableName(queue)
	q := db.DB(service.dbname).Table(table)
	q = q.Between(bigBangDate, db.Now(), db.BetweenOpts{LeftBound: "closed", RightBound: "closed", Index: "next_delivery"})
	q = q.OrderBy("next_delivery")
	q = q.Limit(limit)
	q = q.Update(map[string]interface{}{
		"delivery_count": db.Row.Field("delivery_count").Add(1).Default(1),
		"next_delivery":  db.Now().Add(hold.Seconds()), //add fuzzy
	}, db.UpdateOpts{Durability: "soft", ReturnChanges: true})

	response, err := q.RunWrite(service.session)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	if response.Errors != 0 {
		return nil, errors.New(response.FirstError)
	}

	messages := make([]*Message, 0, response.Replaced)

	if response.Replaced <= 0 {
		return messages, nil
	}

	for _, change := range response.Changes {
		dbm := new(dbMessage)
		err = encoding.Decode(dbm, change.NewValue)
		if err != nil {
			//log.Println("conversion error!")
			continue
		}
		messages = append(messages, &Message{
			Id:            dbm.Id,
			Payload:       dbm.Payload,
			NextDelivery:  dbm.NextDelivery,
			DeliveryCount: dbm.DeliveryCount,
		})

	}

	return messages, nil

}

func existsDB(session *db.Session, dbname string) (error, bool) {
	q := db.DBList().SetIntersection([]interface{}{dbname})
	cursor, err := q.Run(session)
	if err != nil {
		return err, false
	}
	defer cursor.Close()
	var v interface{}

	for cursor.Next(&v) {
		if dbname == v.(string) {
			return nil, true
		}
	}

	return nil, false
}

func existTable(session *db.Session, dbname string, dbtable string) (error, bool) {
	q := db.DB(dbname).TableList().SetIntersection([]interface{}{dbtable})
	cursor, err := q.Run(session)
	if err != nil {
		return err, false
	}
	defer cursor.Close()
	var v interface{}

	for cursor.Next(&v) {
		if dbtable == v.(string) {
			return nil, true
		}
	}

	return nil, false
}
