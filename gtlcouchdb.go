package main

import (
	"errors"
	"time"

	"github.com/rhinoman/couchdb-go"
)

const (
	couchdbStateOpened = 1
	couchdbStateClosed = 2
)

type CouchdbInstance struct {
	state int
	url   string
	conn  *couchdb.Connection
}

func NewCouchdb(strurl string) *CouchdbInstance {
	couchdb := CouchdbInstance{
		state: couchdbStateClosed,
		url:   strurl,
		conn:  nil,
	}
	return &couchdb
}

func (m *CouchdbInstance) openCouchdbConnection() error {
	var timeout = time.Duration(500 * time.Millisecond)
	conn, err := couchdb.NewConnection(m.url, 5984, timeout)
	if err != nil {
		return errors.New("open couchdb db connection failed")
	}
	m.conn = conn
	m.state = couchdbStateOpened
	return nil
}

func (m *CouchdbInstance) closeCouchdbConnection() {

}

func (m *CouchdbInstance) InsertData(db string, data string) error {
	if m.state == couchdbStateClosed {
		return errors.New("connection is error")
	}
	return nil

}

func (m *CouchdbInstance) ReadData(db string, key string) (interface{}, error) {
	if m.state == couchdbStateClosed {
		return nil, errors.New("connection is error")
	}
	return nil, nil
}

func (m *CouchdbInstance) UpdateData(db string, key string, oldVal interface{}, newVal interface{}) (interface{}, error) {
	if m.state == couchdbStateClosed {
		return nil, errors.New("connection is error")
	}
	return nil, nil
}

func (m *CouchdbInstance) DeleteData(db string, key string, value string) (int, error) {
	if m.state == couchdbStateClosed {
		return -1, errors.New("connection is error")
	}
	return 0, nil
}
