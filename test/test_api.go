package main

import (
	"fmt"
	"gtlservice/gtldbservice/mqHelper"
	"log"
	"os"
	"os/signal"
	"strings"
)

func onRead(mqMsg *gtlmqhelper.MQMessage, userData interface{}) {
	log.Println("recv resp message:", string(mqMsg.Body))
}

func testWrite(mq *gtlmqhelper.MQService) {
	var msg = string(`{
    "app_id":"gtlUserDb",
    "req_id":"6e760338-5eb3-4858-a4a7-7eb94211111",
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
	err := mq.DeliveryMsg("text/json", "user_db.req", msg, len(msg))
	if err != nil {
		log.Println("delivery msg error ", err)
	}
	log.Println("deliver write msg success ", msg)
}

func testRead(mq *gtlmqhelper.MQService) {
	var msg = string(`{
    "app_id":"gtlUserDb",
    "req_id":"6e760338-5eb3-4858-a4a7-7eb942255222",
    "method":"READ",
    "key_name":"acc_name",
    "key_value":"maji",
    "data":""}`)
	err := mq.DeliveryMsg("text/json", "user_db.req", msg, len(msg))
	if err != nil {
		log.Println("delivery msg error ", err)
	}
	log.Println("deliver read msg success ", msg)
}

func testDelete(mq *gtlmqhelper.MQService) {
	var msg = string(`{
    "app_id":"gtlUserDb",
    "req_id":"6e760338-5eb3-4858-a4a7-7eb9422553333",
    "method":"DELETE",
    "key_name":"acc_name",
    "key_value":"maji",
    "data":""}`)
	err := mq.DeliveryMsg("text/json", "user_db.req", msg, len(msg))
	if err != nil {
		log.Println("delivery msg error ", err)
	}
	log.Println("deliver delete msg success ", msg)
}

func testUpdate(mq *gtlmqhelper.MQService) {
	var msg = string(`{
    "app_id":"gtlUserDb",
    "req_id":"6e760338-5eb3-4858-a4a7-7eb9422554444",
    "method":"UPDATE",
    "key_name":"acc_name",
    "key_value":"maji",
    "data":{"acc_name":"maji",
            "password":"update",
            "secure_question": "我的生日",
            "secure_answer":"199111110/09/15",
            "email":"te1111111st@gmail.com",
            "phone_number":"186921111110298475"}
    }`)
	err := mq.DeliveryMsg("text/json", "user_db.req", msg, len(msg))
	if err != nil {
		log.Println("delivery msg error ", err)
	}
	log.Println("deliver delete msg success ", msg)
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

	arg_num := len(os.Args)
	fmt.Printf("the num of input is %d\n", arg_num)
	for i := 0; i < arg_num; i++ {
		if strings.EqualFold(os.Args[i], "read") {
			for i := 0; i < 10; i++ {
				go testRead(mq)
			}

		} else if strings.EqualFold(os.Args[i], "write") {
			for i := 0; i < 10; i++ {
				go testWrite(mq)
			}

		} else if strings.EqualFold(os.Args[i], "update") {
			for i := 0; i < 10; i++ {
				go testUpdate(mq)
			}

		} else if strings.EqualFold(os.Args[i], "delete") {
			for i := 0; i < 10; i++ {
				go testDelete(mq)
			}

		} else {
			fmt.Println("args is not valid")
		}

	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	// Block until a signal is received.
	s := <-c
	log.Println("Got signal:", s)
	log.Println("test app quit....")

}
