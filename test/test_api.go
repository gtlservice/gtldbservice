package main

import (
	"gtlservice/gtldbservice/mqHelper"
	"log"
	"os"
	"os/signal"
)

func onRead(mqMsg *gtlmqhelper.MQMessage, userData interface{}) {
	log.Println("recv resp message:", string(mqMsg.Body))
}

func main() {
	mq, err := gtlmqhelper.New("amqp://guest:guest@127.0.0.1:5672", "userdb_exchange", 2)
	if err != nil {
		log.Println("connect mq failed")
		return
	}
	err = mq.CreateQueueAndBind("testqueue", "user_db.resp")
	if err != nil {
		log.Println("create testqueue failed")
		return
	}

	mq.DoConsumer(onRead, mq)
	var msg = string(`{
    "app_id":"gtlUserDb",
    "req_id":"6e760338-5eb3-4858-a4a7-7eb942255e8c",
    "method":"WRITE",
    "key_name":"name",
    "key_value":"maji",
    "data":{"acc_name":"maji",
            "password":"123",
            "secure_question": "我的生日",
            "secure_answer":"1990/09/15",
            "email":"test@gmail.com",
            "phone_number":"186920298475"}
    }`)
	err = mq.DeliveryMsg("text/json", "user_db.req", msg, len(msg))
	if err != nil {
		log.Println("delivery msg error ", err)
	}
	log.Println("deliver msg success ", msg)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	// Block until a signal is received.
	s := <-c
	log.Println("Got signal:", s)
	log.Println("test app quit....")

}
