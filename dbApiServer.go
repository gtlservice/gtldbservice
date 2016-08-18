package main

import (
	"gtlDbAPIService/mqHelper"
	"io/ioutil"
	"log"

	"github.com/bitly/go-simplejson"
)

import "database/sql"
import _ "github.com/go-sql-driver/mysql"

const (
	configFile = "./config.json"
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

func main() {
	config := readConfig()
	if config == nil {
		log.Println("read config file config.json failed in current directory")
		return
	}
	log.Println("config is :", config)
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

	db, err := sql.Open("mysql", "11111111")
	if err != nil {
		log.Println("sql open failed ", err)
		return
	}

	err = mq.DoConsumer(onReadMsg, db)
	if err != nil {
		log.Println("consumer msg failed")
		return
	}
	log.Println("create mq client success!!!")

	log.Println("gtl dbapi server start success...")

}

func onReadMsg(msgType string, content string, contentLen int, userData interface{}) {
	db, ok := userData.(sql.DB)
	if !ok {
		return
	}
	log.Println("readmsg db ", db)

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
