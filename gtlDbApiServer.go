package main

import (
	"errors"
	"fmt"
	"gtlservice/gtldbservice/mqHelper"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/bitly/go-simplejson"
	_ "github.com/go-sql-driver/mysql"
)

const (
	configFile     = "./config.json"
	dbTypemysql    = 1
	dbTypeMongo    = 2
	connStateFree  = 0
	connStateInuse = 1
)

type mqConfig struct {
	MqURL          string
	ExchangeName   string
	ExchangeType   int
	QueueName      string
	ReqRoutingKey  string
	RespRoutingKey string
	dblist         []dbinstance
	appDBTypeMap   map[string]string
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
	state      int //0 free 1 inuse
	stateLock  *sync.Mutex
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

func createJsonData(record []record) (string, error) {
	var json string = `[`
	var objs []string
	for _, v := range record {
		if v.line["id"] == nil {
			continue
		}
		id := v.line["id"]
		accName := v.line["acc_name"]
		password := v.line["password"]
		secure_question := v.line["secure_question"]
		secure_answer := v.line["secure_answer"]
		email := v.line["email"]
		phone_number := v.line["phone_number"]
		obj := fmt.Sprintf(`{"id": %d, "acc_name":"%s", 
			"password":"%s", 
			"secure_question":"%s", 
			"secure_answer":"%s",
		   "email":"%s", 
	       "phone_number":"%s"}`, id, accName, password, secure_question, secure_answer, email, phone_number)
		objs = append(objs, obj)
	}
	jsonObjs := strings.Join(objs, ",")
	json = json + jsonObjs + `]`
	return json, nil
}

//id acc_name password secure_question secure_answer email phone_number
func handleRead(appId, reqId, keyName, keyValue string, data *simplejson.Json, conn *dbconnection) {
	db, ok := conn.connection.(*MysqlInstance)
	if !ok {
		log.Println("read failed ")
		doResponse(reqId, appId, "connection is error", false, 0)
		return
	}
	affect, record, err := db.readUserInfo(keyName, keyValue)
	if err != nil {
		log.Println("read userinfo failed")
		doResponse(reqId, appId, "read error", false, 0)
		return
	}
	if affect == 0 {
		log.Println("read userinfo failed")
		doResponse(reqId, appId, "empty record", false, 0)
		return
	}
	json, err := createJsonData(record)
	if err != nil {
		doResponse(reqId, appId, "convert record to json failed", false, 0)
		return
	}

	doResponseWithData(reqId, appId, "ok", json, true, affect)
}

func handleWrite(appId, reqId, keyName, keyValue string, data *simplejson.Json, conn *dbconnection) {

	var result string
	var isok bool = false
	if conn == nil || conn.connection == nil {
		log.Println("conn ", *conn, " connection ", (*conn).connection)
		doResponse(reqId, appId, "connection is busy and nil", false, 0)
		return
	}

	acc_name, err := data.Get("acc_name").String()
	if err != nil {
		log.Println("get acc_name err", err)
		doResponse(reqId, appId, "acc_name is error", isok, 0)
		return
	}
	password, err := data.Get("password").String()
	if err != nil {
		log.Println("get password err", err)
		doResponse(reqId, appId, "password is error", isok, 0)
		return
	}
	secureQuestion, err := data.Get("secure_question").String()
	if err != nil {
		log.Println("get secure_question failed")
	}
	secureAnswer, err := data.Get("secure_answer").String()
	if err != nil {
		log.Println("get secure_answer failed")
	}
	email, err := data.Get("email").String()
	if err != nil {
		log.Println("get email failed")
		doResponse(reqId, appId, "email is error", isok, 0)
		return
	}
	phoneNumber, err := data.Get("phone_number").String()
	if err != nil {
		log.Println("get phone number failed")
	}

	db, ok := conn.connection.(*MysqlInstance)
	if ok {
		aff, err := db.writeUserInfo(acc_name, password, secureQuestion, secureAnswer, email, phoneNumber)
		conn.stateLock.Lock()
		conn.state = connStateFree
		conn.stateLock.Unlock()
		if err != nil {
			log.Println("write userinfo failed", err)
			result = "insert user info failed"
			isok = false
		} else {
			log.Println("write data success")
			result = "ok"
			isok = true
		}
		doResponse(reqId, appId, result, isok, aff)

	} else {
		log.Println("execute write failed, connection error: ", conn.connection)
	}

}

func handleDelete(appId, reqId, keyName, keyValue string, data *simplejson.Json, conn *dbconnection) {

	if conn == nil || conn.connection == nil {
		doResponse(reqId, appId, "connection is busy and nil", false, 0)
		return
	}
	db, ok := conn.connection.(*MysqlInstance)
	if !ok {
		log.Println("connection error when delete")
		return
	}
	affected, err := db.deleteUserInfo(keyName, keyValue)
	if err != nil {
		log.Println("delete error")
		doResponse(reqId, appId, "delete error", false, 0)
		return
	}
	doResponse(reqId, appId, "ok", true, affected)
}

func handleUpdate(appId, reqId, keyName, keyValue string, data *simplejson.Json, conn *dbconnection) {
	if conn == nil || conn.connection == nil {
		doResponse(reqId, appId, "connection is busy and nil", false, 0)
		return
	}
	db, ok := conn.connection.(*MysqlInstance)
	if !ok {
		log.Println("connection error when update")
		return
	}

	acc_name, err := data.Get("acc_name").String()
	if err != nil {
		log.Println("get acc_name err", err)
		doResponse(reqId, appId, "acc_name is error", false, 0)
		return
	}
	password, err := data.Get("password").String()
	if err != nil {
		log.Println("get password err", err)
		doResponse(reqId, appId, "password is error", false, 0)
		return
	}
	secureQuestion, err := data.Get("secure_question").String()
	if err != nil {
		log.Println("get secure_question failed")
	}
	secureAnswer, err := data.Get("secure_answer").String()
	if err != nil {
		log.Println("get secure_answer failed")
	}
	email, err := data.Get("email").String()
	if err != nil {
		log.Println("get email failed")
		doResponse(reqId, appId, "email is error", false, 0)
		return
	}
	phoneNumber, err := data.Get("phone_number").String()
	if err != nil {
		log.Println("get phone number failed")
	}

	affected, err := db.updateUserInfo(keyName, keyValue, acc_name, password, secureQuestion, secureAnswer, email, phoneNumber)
	if err != nil {
		log.Println("update error")
		doResponse(reqId, appId, "update error", false, 0)
		return
	}
	doResponse(reqId, appId, "ok", true, affected)
}

func makeRespHeader(appId, respId, result, data string) (string, error) {
	header := fmt.Sprintf("{\"app_id\" : \"%s\", \"resp_id\": \"%s\", \"response_result\":\"%s\", \"data\":%s}", appId, respId, result, data)
	return header, nil
}

func doResponseWithData(reqId, appId, stringResult, data string, result bool, affectedRows int) {
	header, err := makeRespHeader(appId, reqId, stringResult, data)
	if err != nil {
		log.Println("make response header failed", header)
		return
	}
	resp, err := simplejson.NewJson([]byte(header))
	if err != nil {
		log.Println("make response json failed . ", header)
		return
	}
	finalResp, err := resp.MarshalJSON()
	if err != nil {
		log.Println("marshaljson failed")
		return
	}
	err = apiServerMQ.DeliveryMsg("text/json", apiServerConfig.RespRoutingKey, string(finalResp), len(finalResp))
	if err != nil {
		log.Println("mq deliver msg failed")
	}
	log.Println("delivery response ", string(finalResp), " ok")
}

func doResponse(reqId, appId, stringResult string, result bool, affectedRows int) {
	ret := fmt.Sprintf("\"affect rows %d\"", affectedRows)
	doResponseWithData(reqId, appId, stringResult, ret, result, affectedRows)
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
	err = mq.CreateQueueAndBind(config.QueueName, config.ReqRoutingKey)
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
		log.Println("dbtype:", dbType)
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
	log.Println("msg content", string(msg.Body))
	json, err := simplejson.NewJson(msg.Body)
	if err != nil {
		log.Println("parse msg to json failed, ", err)
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
	if len(dbConns) == 0 {
		log.Println("can't get connections for appid :", appId)
		return
	}

	log.Println("app conns ", dbConns)

	hash := getHashByKey(keyValue)
	index := hash % len(dbConns)

	var conn dbconnection
	dbConns[index].stateLock.Lock()
	if dbConns[index].state == connStateFree {
		conn = dbConns[index]
		log.Println("connection count ", len(dbConns), " hash index ", index, "conn ", conn)
		conn.state = connStateInuse

	} else {
		log.Println("get connection failed , every connection is busy, state ", dbConns[index].state)
	}
	dbConns[index].stateLock.Unlock()

	switch method {
	case "READ":
		dbHandleFunc["READ"](appId, reqId, keyName, keyValue, dataJson, &conn)
	case "WRITE":
		log.Println("connection!!!!! ", &conn)
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
	log.Println("on read msg conns ", dbconns)
	processMqMessage(msg, dbconns)
}

func doMySQLConnection(url string) interface{} {
	mysqlInst := NewMysql()
	err := mysqlInst.openMysqlConnection(url)
	if err != nil {
		log.Println("connect ", url, " failed error:", err)
		return nil
	}
	log.Println("connect ", url, " success!!")
	return mysqlInst
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
			conn.stateLock = new(sync.Mutex)
			conn.state = connStateFree
		case "mongo":
			conn.connection = doNoSQLConnection(db.Dburl)
			conn.connType = dbTypeMongo
			conn.hashIndex = db.Index
			conn.stateLock = new(sync.Mutex)
			conn.state = connStateFree
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

	config.ReqRoutingKey, err = json.Get("ReqRoutingKey").String()
	if err != nil {
		log.Println("parse req routingkey failed")
		return nil
	}

	config.RespRoutingKey, err = json.Get("RespRoutingKey").String()
	if err != nil {
		log.Println("parse resp routingkey failed")
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
