package main

import (
	"encoding/json"
	"gtlDbAPIService/mqHelper"
	"io/ioutil"
	"log"
)

const (
	configFile = "./config.json"
)

type mqConfig struct {
	MqURL        string
	ExchangeName string
	ExchangeType int
	QueueName    string
	RoutingKey   string
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
	err = mq.DoConsumer(onReadMsg)
	if err != nil {
		log.Println("consumer msg failed")
		return
	}

	log.Println("gtl dbapi server start success...")

}

func onReadMsg(msgType string, content string, contentLen int) {

}

func readConfig() *mqConfig {
	var content []byte
	var config mqConfig
	var err error
	content, err = ioutil.ReadFile(configFile)
	if err != nil {
		log.Println(err)
		return nil
	}
	log.Printf("content is : %s", content)
	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Println(err)
		return nil
	}
	return &config
}
