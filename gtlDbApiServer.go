package main

import (
	"errors"
	"gtlservice/gtldbservice/mqHelper"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/bitly/go-simplejson"
	_ "github.com/go-sql-driver/mysql"
)

const (
	configFile  = "./config.json"
	dbTypemysql = 1
	dbTypeMongo = 2
)

type mqConfig struct {
	MqURL        string
	ExchangeName string
	ExchangeType int
	QueueName    string
	RoutingKey   string
	dblist       []dbinstance
}

type dbinstance struct {
	Dburl  string
	Index  int
	DbType string
}

type dbconnection struct {
	connection interface{}
	connType   int
	hashIndex  int
}

func main() {
	config := readConfig()
	if config == nil {
		log.Println("read config file config.json failed in current directory")
		return
	}

	conns, err := initDbConnection(config)
	if err != nil {
		log.Println("init db connection failed")
		return
	}

	mq, err := gtlmqhelper.New(config.MqURL, config.ExchangeName, config.ExchangeType)
	if err != nil {
		log.Println("create mq instance failed")
		return
	}
	err = mq.CreateQueueAndBind(config.QueueName, config.RoutingKey)
	if err != nil {
		log.Println("create queue failed")
		return
	}

	err = mq.DoConsumer(onReadMsg, conns)
	if err != nil {
		log.Println("consumer msg failed")
		return
	}
	log.Println("create mq client success!!!")
	log.Println("gtl dbapi server start success...")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	// Block until a signal is received.
	s := <-c
	log.Println("Got signal:", s)
	log.Println("dbApiServer quit....")

}

func getHashByKey(key string) int {
	var hash, index int
	index = 1
	strings.FieldsFunc(key, func(c rune) bool {
		hash += index * int(c)
		index++
		return false
	})
	return hash
}

//接受从rabbitmq-server投递过来的消息
func onReadMsg(msg *gtlmqhelper.MQMessage, userData interface{}) {
	dbconns, ok := userData.([]dbconnection)
	if !ok {
		return
	}

	//getHashIndexByKey()
	//log.Println("readmsg db ", dbconnection)
	//dbconns

}

func doMySQLConnection(url string) interface{} {
	return nil
}

func doNoSQLConnection(url string) interface{} {
	return nil
}

func initDbConnection(config *mqConfig) ([]dbconnection, error) {
	var dbconnectons []dbconnection
	dbs := config.dblist
	for _, db := range dbs {
		var conn dbconnection
		switch db.DbType {
		case "mysql":
			conn.connection = doMySQLConnection(db.Dburl)
			conn.connType = dbTypemysql
			conn.hashIndex = db.Index
		case "mongo":
			conn.connection = doNoSQLConnection(db.Dburl)
			conn.connType = dbTypeMongo
			conn.hashIndex = db.Index
		default:
			log.Println("unknown sql type", db.DbType)
			return nil, errors.New("unknown sql type")
		}
		dbconnectons = append(dbconnectons, conn)
	}
	return dbconnectons, nil
}

//read config.json file and parse
func readConfig() *mqConfig {
	var content []byte
	var config mqConfig
	var err error
	var json *simplejson.Json
	content, err = ioutil.ReadFile(configFile)
	if err != nil {
		log.Println(err)
		return nil
	}
	//log.Printf("content is : %s", content)
	json, err = simplejson.NewJson([]byte(content))
	if err != nil {
		log.Println("new simple json failed ", err)
		return nil
	}
	config.MqURL, err = json.Get("MqURL").String()
	if err != nil {
		log.Println("parse mqurl failed")
		return nil
	}
	config.ExchangeName, err = json.Get("ExchangeName").String()
	if err != nil {
		log.Println("parse exchange name failed")
		return nil
	}

	config.ExchangeType, err = json.Get("ExchangeType").Int()
	if err != nil {
		log.Println("parse exchangetype failed")
		return nil
	}

	config.QueueName, err = json.Get("QueueName").String()
	if err != nil {
		log.Println("parse queuename failed ")
		return nil
	}

	config.RoutingKey, err = json.Get("RoutingKey").String()
	if err != nil {
		log.Println("parse routingkey failed")
		return nil
	}
	dblist := json.Get("dblist")
	var dbs = make([]dbinstance, 0)
	for i := 0; dblist.GetIndex(i) != nil; i++ {
		var db dbinstance
		dburl, err := dblist.GetIndex(i).Get("dburl").String()
		if err != nil {
			break
		}
		db.Dburl = dburl
		index, err := dblist.GetIndex(i).Get("index").Int()
		if err != nil {
			break
		}
		db.Index = index
		dbtype, err := dblist.GetIndex(i).Get("dbtype").String()
		if err != nil {
			break
		}
		db.DbType = dbtype
		dbs = append(dbs, db)
		log.Println("get db", db)
	}

	config.dblist = dbs

	return &config
}
