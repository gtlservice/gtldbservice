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
	appDBTypeMap map[string]string
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

type handleFunc func(appId, reqId, keyName, keyValue string, data *simplejson.Json, conn *dbconnection)

var dbHandleFunc = map[string]handleFunc{
	"READ":   handleRead,
	"WRITE":  handleWrite,
	"DELETE": handleDelete,
	"UPDATE": handleUpdate,
}

var apiServerConfig *mqConfig
var apiServerMQ *gtlmqhelper.MQService

func handleRead(appId, reqId, keyName, keyValue string, data *simplejson.Json, conn *dbconnection) {

}

func handleWrite(appId, reqId, keyName, keyValue string, data *simplejson.Json, conn *dbconnection) {

}

func handleDelete(appId, reqId, keyName, keyValue string, data *simplejson.Json, conn *dbconnection) {

}

func handleUpdate(appId, reqId, keyName, keyValue string, data *simplejson.Json, conn *dbconnection) {

}

func main() {
	config := readConfig()
	if config == nil {
		log.Println("read config file config.json failed in current directory")
		return
	}
	apiServerConfig = config
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
	apiServerMQ = mq

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

func getAppDbs(appId string, conns []dbconnection) []dbconnection {
	dbType := apiServerConfig.appDBTypeMap[appId]
	var dbTypeIndex int
	switch dbType {
	case "mysql":
		dbTypeIndex = dbTypemysql
	case "mongo":
		dbTypeIndex = dbTypeMongo
	default:
		dbTypeIndex = -1
	}
	var selectedConns []dbconnection
	for _, v := range conns {
		if v.connType == dbTypeIndex {
			selectedConns = append(selectedConns, v)
		}
	}
	return selectedConns
}

func processMqMessage(msg *gtlmqhelper.MQMessage, conns []dbconnection) {
	json, err := simplejson.NewJson([]byte(msg.Body))
	if err != nil {
		log.Println("parse msg to json failed")
		return
	}
	appId, err := json.Get("app_id").String()
	if err != nil {
		log.Println("get app_id  failed")
		return
	}
	reqId, err := json.Get("req_id").String()
	if err != nil {
		log.Println("get req_id  failed")
		return
	}
	method, err := json.Get("method").String()
	if err != nil {
		log.Println("get method  failed")
		return
	}
	keyName, err := json.Get("key_name").String()
	if err != nil {
		log.Println("get key_name  failed")
		return
	}
	keyValue, err := json.Get("key_value").String()
	if err != nil {
		log.Println("get key_value  failed")
		return
	}
	dataJson := json.Get("data")
	if dataJson == nil {
		log.Println("get data  failed")
		return
	}
	dbConns := getAppDbs(appId, conns)
	hash := getHashByKey(keyValue)
	index := hash % len(dbConns)
	conn := dbConns[index]
	switch method {
	case "READ":
		dbHandleFunc["READ"](appId, reqId, keyName, keyValue, dataJson, &conn)
	case "WRITE":
		dbHandleFunc["WRITE"](appId, reqId, keyName, keyValue, dataJson, &conn)
	case "DELETE":
		dbHandleFunc["DELETE"](appId, reqId, keyName, keyValue, dataJson, &conn)
	case "UPDATE":
		dbHandleFunc["UPDATE"](appId, reqId, keyName, keyValue, dataJson, &conn)
	default:
		log.Println("unknown method")
	}
}

//接受从rabbitmq-server投递过来的消息
func onReadMsg(msg *gtlmqhelper.MQMessage, userData interface{}) {
	dbconns, ok := userData.([]dbconnection)
	if !ok {
		return
	}
	processMqMessage(msg, dbconns)
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
	config.appDBTypeMap = map[string]string{}

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
	dblist := json.Get("DBlist")
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
	//parse dbtype <---->app map section
	dbMap := json.Get("AppDBTypeMap")
	if dbMap == nil {
		log.Println("get app2db	map section failed")
		return nil
	}

	for j := 0; dbMap.GetIndex(j) != nil; j++ {
		app2db, err := dbMap.GetIndex(j).Map()
		if err != nil {
			break
		}
		for k, v := range app2db {
			s, ok := v.(string)
			if ok {
				config.appDBTypeMap[k] = s
				log.Println("app :", k, "dbtype", s)
			} else {
				log.Println("get app ", k, "failed")
			}
		}
	}

	return &config
}
