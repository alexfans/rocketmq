package rocketmq

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
)

const (
	CompressedFlag          = (0x1 << 0)
	MultiTagsFlag           = (0x1 << 1)
	TransactionNotType      = (0x0 << 2)
	TransactionPreparedType = (0x1 << 2)
	TransactionCommitType   = (0x2 << 2)
	TransactionRollbackType = (0x3 << 2)
)

const (
	NameValueSeparator = 1 + iota
	PropertySeparator
)

const (
	CharacterMaxLength = 255
)

type Message struct {
	Topic      string
	Flag       int32
	properties map[string]string
	Body       []byte
}

func NewMessage(topic string, body []byte) *Message {
	return &Message{
		Topic:      topic,
		Body:       body,
		properties: make(map[string]string),
	}
}
func (m *Message) SetTags(tags string) {
	m.PutProperty(MessageConst.PropertyTags, tags)
}

func (m *Message) GetTags() string {
	return m.GetProperty(MessageConst.PropertyKeys)
}

func (m *Message) SetKeys(keys string) {
	m.PutProperty(MessageConst.PropertyKeys, keys)
}

func (m *Message) GetKeys() string {
	return m.GetProperty(MessageConst.PropertyTags)
}

func (m *Message) SetBuyerId(buyerId string) {
	m.PutProperty(MessageConst.PropertyBuyerId, buyerId)
}

func (m *Message) GetBuyerId() string {
	return m.GetProperty(MessageConst.PropertyBuyerId)
}

func (m *Message) PutProperty(name string, value string) {
	if m.properties == nil {
		m.properties = make(map[string]string)
	}
	m.properties[name] = value
}
func (m *Message) getDelayTimeLevel() int {
	t = m.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL)
	if t != "" {
		l, err := strconv.Atoi(t)
		if err != nil {
			return 0
		}
		return l
	}

	return 0
}

func (m *Message) SetDelayTimeLevel(int level) {
	m.PutProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL, strconv.Itoa(level))
}

func (m *Message) GetProperties() map[string]string {
	return m.properties
}

func (m *Message) SetProperties(properties map[string]string) {
	m.properties = properties
}

func (m *Message) ClearProperty(name string) {
	_, ok := m.properties[name]
	if ok {
		delete(m.properties, name)
	}
}

func (m *Message) GetProperty(name string) string {
	if m.properties == nil {
		return ""
	}
	values, ok := m.properties[name]
	if !ok {
		return ""
	}
	return values
}

func (m *Message) PutUserProperty(name string, value string) error {
	if _, ok := MessageConst.systemKeySet[name]; !ok {
		return errors.New(fmt.Sprintf("The Property<%s> is used by system, input another please", name))
	}
	if name == "" || value == "" {
		return errors.New("The name or value of property can not be null or blank string!")
	}

	m.PutProperty(name, value)
	return nil
}

func (m *Message) GetUserProperty(name string) string {
	return m.GetProperty(name)
}

type MessageExt struct {
	Message
	QueueId       int32
	StoreSize     int32
	QueueOffset   int64
	SysFlag       int32
	BornTimestamp int64
	// bornHost
	StoreTimestamp int64
	// storeHost
	MsgId                     string
	CommitLogOffset           int64
	BodyCRC                   int32
	ReconsumeTimes            int32
	PreparedTransactionOffset int64
}

func decodeMessage(data []byte) []*MessageExt {
	buf := bytes.NewBuffer(data)
	var storeSize, magicCode, bodyCRC, queueId, flag, sysFlag, reconsumeTimes, bodyLength, bornPort, storePort int32
	var queueOffset, physicOffset, preparedTransactionOffset, bornTimeStamp, storeTimestamp int64
	var topicLen byte
	var topic, body, properties, bornHost, storeHost []byte
	var propertiesLength int16

	var propertiesMap map[string]string

	msgs := make([]*MessageExt, 0, 32)
	for buf.Len() > 0 {
		msg := new(MessageExt)
		binary.Read(buf, binary.BigEndian, &storeSize)
		binary.Read(buf, binary.BigEndian, &magicCode)
		binary.Read(buf, binary.BigEndian, &bodyCRC)
		binary.Read(buf, binary.BigEndian, &queueId)
		binary.Read(buf, binary.BigEndian, &flag)
		binary.Read(buf, binary.BigEndian, &queueOffset)
		binary.Read(buf, binary.BigEndian, &physicOffset)
		binary.Read(buf, binary.BigEndian, &sysFlag)
		binary.Read(buf, binary.BigEndian, &bornTimeStamp)
		bornHost = make([]byte, 4)
		binary.Read(buf, binary.BigEndian, &bornHost)
		binary.Read(buf, binary.BigEndian, &bornPort)
		binary.Read(buf, binary.BigEndian, &storeTimestamp)
		storeHost = make([]byte, 4)
		binary.Read(buf, binary.BigEndian, &storeHost)
		binary.Read(buf, binary.BigEndian, &storePort)
		binary.Read(buf, binary.BigEndian, &reconsumeTimes)
		binary.Read(buf, binary.BigEndian, &preparedTransactionOffset)
		binary.Read(buf, binary.BigEndian, &bodyLength)
		if bodyLength > 0 {
			body = make([]byte, bodyLength)
			binary.Read(buf, binary.BigEndian, body)

			if (sysFlag & CompressedFlag) == CompressedFlag {
				b := bytes.NewReader(body)
				z, err := zlib.NewReader(b)
				if err != nil {
					fmt.Println(err)
					return nil
				}
				defer z.Close()
				body, err = ioutil.ReadAll(z)
				if err != nil {
					fmt.Println(err)
					return nil
				}
			}

		}
		binary.Read(buf, binary.BigEndian, &topicLen)
		topic = make([]byte, 0)
		binary.Read(buf, binary.BigEndian, &topic)
		binary.Read(buf, binary.BigEndian, &propertiesLength)
		if propertiesLength > 0 {
			properties = make([]byte, propertiesLength)
			binary.Read(buf, binary.BigEndian, &properties)
			propertiesMap = make(map[string]string)
			json.Unmarshal(properties, &propertiesMap)
		}

		if magicCode != -626843481 {
			fmt.Printf("magic code is error %d", magicCode)
			return nil
		}

		msg.Topic = string(topic)
		msg.QueueId = queueId
		msg.SysFlag = sysFlag
		msg.QueueOffset = queueOffset
		msg.BodyCRC = bodyCRC
		msg.StoreSize = storeSize
		msg.BornTimestamp = bornTimeStamp
		msg.ReconsumeTimes = reconsumeTimes
		msg.Flag = flag
		//msg.commitLogOffset=physicOffset
		msg.StoreTimestamp = storeTimestamp
		msg.PreparedTransactionOffset = preparedTransactionOffset
		msg.Body = body
		msg.properties = propertiesMap

		msgs = append(msgs, msg)
	}

	return msgs
}

func messageProperties2String(properties map[string]string) string {
	StringBuilder := bytes.NewBuffer([]byte{})
	if properties != nil && len(properties) != 0 {
		for k, v := range properties {
			binary.Write(StringBuilder, binary.BigEndian, k)                  // 4
			binary.Write(StringBuilder, binary.BigEndian, NameValueSeparator) // 4
			binary.Write(StringBuilder, binary.BigEndian, v)                  // 4
			binary.Write(StringBuilder, binary.BigEndian, PropertySeparator)  // 4
		}
	}
	return StringBuilder.String()
}

func (msg Message) checkMessage(producer *DefaultProducer) (err error) {
	if err = checkTopic(msg.Topic); err != nil {
		if len(msg.Body) == 0 {
			err = errors.New("ResponseCode:" + strconv.Itoa(MsgIllegal) + ", the message body is null")
		} else if len(msg.Body) > producer.maxMessageSize {
			err = errors.New("ResponseCode:" + strconv.Itoa(MsgIllegal) + ", the message body size over max value, MAX:" + strconv.Itoa(producer.maxMessageSize))
		}
	}
	return
}

func checkTopic(topic string) (err error) {
	if topic == "" {
		err = errors.New("the specified topic is blank")
	}
	if len(topic) > CharacterMaxLength {
		err = errors.New("the specified topic is longer than topic max length 255")
	}
	if topic == DefaultTopic {
		err = errors.New("the topic[" + topic + "] is conflict with default topic")
	}
	return
}
