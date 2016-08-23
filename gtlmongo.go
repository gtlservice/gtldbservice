package main

import (
	"errors"

	"github.com/go-mgo/mgo"
)

const (
	mongoStateOpened = 1
	mongoStateClosed = 2
)

type MongoInstance struct {
	state int
	url   string
	conn  *mgo.Session
}

func NewMongo() *MongoInstance {
	mongo := new(MongoInstance)
	mongo.state = mongoStateClosed
	return mongo
}

func (m *MongoInstance) openMongoDbConnection(url string) error {
	session, err := mgo.Dial(url)
	if err != nil {
		return errors.New("open mongo db connection failed")
	}
	m.conn = session
	m.state = mongoStateOpened
	return nil
}

func (m *MongoInstance) closeMongoDbConnection() {
	m.conn.Close()
	m.state = mongoStateClosed
}

func (m *MongoInstance) InsertData(db string, data string) error {
	return nil

}

func (m *MongoInstance) ReadData(db string, key string) (interface{}, error) {
	return nil, nil
}

func (m *MongoInstance) UpdateData(db string, key string, oldVal interface{}, newVal interface{}) (interface{}, error) {
	return nil, nil
}

func (m *MongoInstance) DeleteData(db string, key string, value string) (int, error) {
	return 0, nil
}
