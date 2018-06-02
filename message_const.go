package rocketmq

type messageConst struct {
	PropertyKeys                      string
	PropertyTags                      string
	PropertyWaitStoreMsgOk            string
	PropertyDelayTimeLevel            string
	PropertyRetryTopic                string
	PropertyRealTopic                 string
	PropertyRealQueueId               string
	PropertyTransactionPrepared       string
	PropertyProducerGroup             string
	PropertyMinOffset                 string
	PropertyMaxOffset                 string
	PropertyBuyerId                   string
	PropertyOriginMessageId           string
	PropertyTransferFlag              string
	PropertyCorrectionFlag            string
	PropertyMq2Flag                   string
	PropertyReconsumeTime             string
	PropertyMsgRegion                 string
	PropertyUniqClientMessageIdKeyidx string
	PropertyMaxReconsumeTimes         string
	PropertyConsumeStartTimeStamp     string

	KeySeparator string
	systemKeySet map[string]struct{}
}

var MessageConst = &messageConst{
	PropertyKeys:                      "KEYS",
	PropertyTags:                      "TAGS",
	PropertyWaitStoreMsgOk:            "WAIT",
	PropertyDelayTimeLevel:            "DELAY",
	PropertyRetryTopic:                "RETRY_TOPIC",
	PropertyRealTopic:                 "REAL_TOPIC",
	PropertyRealQueueId:               "REAL_QID",
	PropertyTransactionPrepared:       "TRAN_MSG",
	PropertyProducerGroup:             "PGROUP",
	PropertyMinOffset:                 "MIN_OFFSET",
	PropertyMaxOffset:                 "MAX_OFFSET",
	PropertyBuyerId:                   "BUYER_ID",
	PropertyOriginMessageId:           "ORIGIN_MESSAGE_ID",
	PropertyTransferFlag:              "TRANSFER_FLAG",
	PropertyCorrectionFlag:            "CORRECTION_FLAG",
	PropertyMq2Flag:                   "MQ2_FLAG",
	PropertyReconsumeTime:             "RECONSUME_TIME",
	PropertyMsgRegion:                 "MSG_REGION",
	PropertyUniqClientMessageIdKeyidx: "UNIQ_KEY",
	PropertyMaxReconsumeTimes:         "MAX_RECONSUME_TIMES",
	PropertyConsumeStartTimeStamp:     "CONSUME_START_TIME",

	KeySeparator: "",
}

func init() {
	var systemKeySet = make(map[string]struct{}, 20)
	systemKeySet[MessageConst.PropertyKeys] = struct{}{}
	systemKeySet[MessageConst.PropertyTags] = struct{}{}
	systemKeySet[MessageConst.PropertyWaitStoreMsgOk] = struct{}{}
	systemKeySet[MessageConst.PropertyDelayTimeLevel] = struct{}{}
	systemKeySet[MessageConst.PropertyRetryTopic] = struct{}{}
	systemKeySet[MessageConst.PropertyRealTopic] = struct{}{}
	systemKeySet[MessageConst.PropertyRealTopic] = struct{}{}
	systemKeySet[MessageConst.PropertyRealQueueId] = struct{}{}
	systemKeySet[MessageConst.PropertyTransactionPrepared] = struct{}{}
	systemKeySet[MessageConst.PropertyProducerGroup] = struct{}{}
	systemKeySet[MessageConst.PropertyMinOffset] = struct{}{}
	systemKeySet[MessageConst.PropertyMaxOffset] = struct{}{}
	systemKeySet[MessageConst.PropertyBuyerId] = struct{}{}
	systemKeySet[MessageConst.PropertyOriginMessageId] = struct{}{}
	systemKeySet[MessageConst.PropertyTransferFlag] = struct{}{}
	systemKeySet[MessageConst.PropertyCorrectionFlag] = struct{}{}
	systemKeySet[MessageConst.PropertyMq2Flag] = struct{}{}
	systemKeySet[MessageConst.PropertyReconsumeTime] = struct{}{}
	systemKeySet[MessageConst.PropertyMsgRegion] = struct{}{}
	systemKeySet[MessageConst.PropertyUniqClientMessageIdKeyidx] = struct{}{}
	systemKeySet[MessageConst.PropertyMaxReconsumeTimes] = struct{}{}
	systemKeySet[MessageConst.PropertyConsumeStartTimeStamp] = struct{}{}

	MessageConst.systemKeySet = systemKeySet
}
