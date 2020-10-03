package pgevents

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

type Listener struct {
	stop      chan bool
	db        *sql.DB
	pql       *pq.Listener
	callbacks []Callback
}

type TableEvent struct {
	Table  string
	Action string
	Data   string
}

type Callback func(*TableEvent)

func OpenListener(connectionString string) (*Listener, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open sql connection")
	}

	if _, err := db.Exec(procedure()); err != nil {
		return nil, errors.Wrap(err, "failed to create postgres notify function")
	}

	l := &Listener{
		stop: make(chan bool),
		db:   db,
		pql:  pq.NewListener(connectionString, 10*time.Second, time.Minute, nil),
	}

	if err := l.start(); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Listener) Attach(table string) error {
	if _, err := l.db.Exec(trigger(table)); err != nil {
		return errors.Wrap(err, "failed to attach listener")
	}
	return nil
}

func (l *Listener) OnEvent(cb Callback) {
	l.callbacks = append(l.callbacks, cb)
}

func (l *Listener) start() error {
	if err := l.pql.Listen("pgevents_event"); err != nil {
		return errors.Wrap(err, "failed to listen to postgres events")
	}

	go func() {
		for {
			select {
			case <-l.stop:
				log.Println("finished listening for events")
				return
			case n := <-l.pql.Notify:
				log.Printf("received data from channel: %s\n", n.Channel)
				l.emit(n)
			case <-time.After(time.Minute):
				log.Println("no events received for 1 minute: checking connection")
				go l.pql.Ping()
			}
		}
	}()

	return nil
}

func (l *Listener) emit(notification *pq.Notification) {
	event := &TableEvent{}

	if err := json.Unmarshal([]byte(notification.Extra), event); err != nil {
		log.Println(errors.Wrap(err, "failed to unmarshal table event"))
		return
	}

	for _, cb := range l.callbacks {
		cb(event)
	}
}

func (l *Listener) Close() error {
	l.stop <- true
	l.pql.Close()
	l.db.Close()
	return nil
}
