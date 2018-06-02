package rocketmq

const (
	SendStatusOK = iota
	SendStatusFlushDiskTimeout
	SendStatusFlushSlaveTimeout
	SendStatusSlaveNotAvailable
)

type SendResult struct {
	sendStatus    int
	msgId         string
	messageQueue  *MessageQueue
	queueOffset   int64
	transactionId string
	offsetMsgId   string
	regionId      string
}

func (sr *SendResult) SendStatus() int {
	return sr.sendStatus
}

func (sr *SendResult) MsgId() string {
	return sr.msgId
}

func (sr *SendResult) MessageQueue() *MessageQueue {
	return sr.messageQueue
}

func (sr *SendResult) QueueOffset() int64 {
	return sr.queueOffset
}

func (sr *SendResult) TransactionId() string {
	return sr.transactionId
}

func (sr *SendResult) OffsetMsgId() string {
	return sr.offsetMsgId
}

func (sr *SendResult) RegionId() string {
	return sr.regionId
}

func NewSendResult(sendStatus int, msgId string, offsetMsgId string, messageQueue *MessageQueue, queueOffset int64) *SendResult {
	return &SendResult{
		sendStatus:   sendStatus,
		msgId:        msgId,
		offsetMsgId:  offsetMsgId,
		messageQueue: messageQueue,
		queueOffset:  queueOffset,
	}
}

func (s *SendResult) SendResult(SendStatus int, msgId string, messageQueue MessageQueue, queueOffset uint64,
	transactionId string, offsetMsgId string, regionId string) (ok bool) {
	return
}
