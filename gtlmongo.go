package main

import (
	"github.com/go-mgo/mgo"
    "errors"
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



func NewMongo() *MongoInstance{
    mongo := new(MongoInstance)
    mongo.state = mongoStateClosed
    return mongo
}

func (m *MongoInstance)openMongoDbConnection(url string) error{
    m.conn, err := mgo.Dial(url)
    if err != nil {
        return errors.New("open mongo db connection failed")
    }
    m.state = mongoStateOpened
    return nil
}


func (m *MongoInstance)closeMongoDbConnection(){
    m.conn.Close()
    m.state = mongoStateClosed
}

func (m *MongoInstance)InsertData(db string, data string) error {

}


func (m *MongoInstance)ReadData(db string, key string) (interface{}, error){

}

func (m *MongoInstance)UpdateData(db string, key string, oldVal interface{}, newVal interface{}) (interface{}, error){

}


func m *MongoInstance)DeleteData(db string, key string, value string) (int, error){
    
}

