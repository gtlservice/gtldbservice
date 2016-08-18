package gtlmqhelper

import "github.com/streadway/amqp"
import "log"
import "errors"

const (
	EXCHANGE_TYPE_DIRECT = 1
	EXCHANGE_TYPE_TOPIC  = 2
	EXCHANGE_TYPE_FANOUT = 3
)

//MQService ..
type MQService struct {
	amqpConn      *amqp.Connection
	amqpWriteChan *amqp.Channel
	amqpReadChan  *amqp.Channel
	amqpReadQueue *amqp.Queue
	exchangeName  string
	readQueueName string
}

var exchangeTypeMap = map[int]string{EXCHANGE_TYPE_DIRECT: "direct", EXCHANGE_TYPE_TOPIC: "topic", EXCHANGE_TYPE_FANOUT: "fanout"}

//New a mqservice object
func New(connectionURL string, exchangeName string, exchangeType int) (*MQService, error) {
	mq := new(MQService)
	var readChan, writeChan *amqp.Channel
	conn, err := amqp.Dial(connectionURL)
	if err != nil {
		log.Println("connection url failed :", connectionURL)
		return nil, errors.New("connect amqp server failed")
	}
	mq.amqpConn = conn
	readChan, err = mq.amqpConn.Channel()
	if err != nil {
		log.Println("create read channel failed")
		return nil, errors.New("create read channel failed")
	}
	writeChan, err = mq.amqpConn.Channel()
	if err != nil {
		log.Println("create write channel failed")
		return nil, errors.New("create write channel failed")
	}
	mq.amqpReadChan = readChan
	mq.amqpWriteChan = writeChan
	mq.exchangeName = exchangeName
	err = mq.amqpWriteChan.ExchangeDeclare(exchangeName, exchangeTypeMap[exchangeType], true, false, false, false, nil)
	if err != nil {
		return nil, errors.New("create exchange failed")
	}
	return mq, nil
}

//Clean clean the resource
func (mq *MQService) Clean() {
	mq.amqpReadChan.Close()
	mq.amqpWriteChan.Close()
	mq.amqpConn.Close()
}

//CreateQueueAndBind bind queue to exchange with routing key
func (mq *MQService) CreateQueueAndBind(queueName string, routingKey string) error {
	queue, err := mq.amqpWriteChan.QueueDeclare(queueName, false, false, true, false, nil)
	if err != nil {
		return errors.New("create queue failed")
	}
	err = mq.amqpWriteChan.QueueBind(queueName, routingKey, mq.exchangeName, false, nil)
	if err != nil {
		return errors.New("bind queue to exchange failed")
	}
	mq.amqpReadQueue = &queue
	mq.readQueueName = queueName
	return nil
}

func doMsgDelivery(msgs <-chan amqp.Delivery, mq *MQService, consumerCallback func(msgType string, content string, contentLen int)) {
	for msg := range msgs {
		msg.Ack(false)
		contentType := msg.ContentType
		content := string(msg.Body)
		len := len(msg.Body)
		consumerCallback(contentType, content, len)
	}
}

//DoConsumer ..
func (mq *MQService) DoConsumer(consumerCallback func(msgType string, content string, contentLen int)) error {
	Msg, err := mq.amqpReadChan.Consume(mq.readQueueName, mq.readQueueName, false, false, false, false, nil)
	if err != nil {
		return errors.New("consumer failed")
	}
	go doMsgDelivery(Msg, mq, consumerCallback)
	return nil
}

//DeliveryMsg msg to bind exchange
func (mq *MQService) DeliveryMsg(msgType string, routingKey string, msg string, len int) error {
	deliverMsg := amqp.Publishing{
		ContentType: msgType,
		Body:        []byte(msg),
	}
	err := mq.amqpWriteChan.Publish(mq.exchangeName, routingKey, false, false, deliverMsg)
	if err != nil {
		return errors.New("publish msg failed")
	}
	return nil
}

//DeliveryMsgToExchange ..
func (mq *MQService) DeliveryMsgToExchange(exchangeName string, msgType string, routingKey string, msg string, len int) error {
	deliverMsg := amqp.Publishing{
		ContentType: msgType,
		Body:        []byte(msg),
	}
	err := mq.amqpWriteChan.Publish(exchangeName, routingKey, false, false, deliverMsg)
	if err != nil {
		return errors.New("publish msg failed")
	}
	return nil
}
