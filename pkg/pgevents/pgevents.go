package pgevents

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Listener struct {
	stop               chan bool
	db                 *sql.DB
	pql                *pq.Listener
	eventCallbacks     []OnEvent
	reconnectCallbacks []OnReconnect
}

type TableEvent struct {
	Table  string
	Action string
	Data   string
}

type OnEvent func(*TableEvent)

type OnReconnect func()

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

func (l *Listener) OnEvent(cb OnEvent) {
	l.eventCallbacks = append(l.eventCallbacks, cb)
}

func (l *Listener) OnReconnect(cb OnReconnect) {
	l.reconnectCallbacks = append(l.reconnectCallbacks, cb)
}

func (l *Listener) start() error {
	if err := l.pql.Listen("pgevents_event"); err != nil {
		return errors.Wrap(err, "failed to listen to postgres events")
	}

	go func() {
		for {
			select {
			case <-l.stop:
				logrus.Debug("finished listening for events")
				return
			case notification := <-l.pql.NotificationChannel():
				if notification != nil {
					logrus.Debugf("received data from channel: %s\n", notification.Channel)
					l.emitEvent(notification)
				} else {
					// a nil notification is documented to mean that
					// the connection has been lost and then re-established
					// i.e. a reconnect occurred and some notifications may
					// have been missed.
					logrus.Debug("received nil from channel indicating reconnect")
					l.emitReconnect()
				}
			case <-time.After(1 * time.Minute):
				logrus.Debug("no events received for 1 minute: checking connection")
				go func() {
					if err := l.pql.Ping(); err != nil {
						logrus.Error(errors.Wrap(err, "pgevents ping returned an error"))
					}
				}()
			}
		}
	}()

	return nil
}

func (l *Listener) emitEvent(notification *pq.Notification) {
	event := &TableEvent{}

	if err := json.Unmarshal([]byte(notification.Extra), event); err != nil {
		logrus.Error(errors.Wrap(err, "failed to unmarshal table event"))
		return
	}

	for _, cb := range l.eventCallbacks {
		cb(event)
	}
}

func (l *Listener) emitReconnect() {
	for _, cb := range l.reconnectCallbacks {
		cb()
	}
}

func (l *Listener) Close() error {
	l.stop <- true
	l.pql.Close()
	l.db.Close()
	return nil
}
