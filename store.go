package rocketmq

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/user"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	MemoryFirstThenStore = 0
	ReadFromMemory       = 1
	ReadFromStore        = 2
)

type OffsetStore interface {
	load() error
	updateOffset(mq *MessageQueue, offset int64, increaseOnly bool)
	readOffset(mq *MessageQueue, flag int) int64
	persistAll(mqs []MessageQueue)
	//persist(mq MessageQueue)
	//removeOffset(mq MessageQueue)
	//cloneOffsetTable(topic string) map[MessageQueue]int64
	OffsetTable() map[MessageQueue]int64
}

type RemoteOffsetStore struct {
	groupName       string
	mqClient        *MqClient
	offsetTable     map[MessageQueue]int64
	offsetTableLock sync.RWMutex
}

func NewRemoteOffsetStore(mqClient *MqClient, groupName string, offsetTable map[MessageQueue]int64) *RemoteOffsetStore {
	return &RemoteOffsetStore{
		groupName:   groupName,
		mqClient:    mqClient,
		offsetTable: offsetTable,
	}
}

func (r *RemoteOffsetStore) load() error {
	return nil
}

func (r *RemoteOffsetStore) readOffset(mq *MessageQueue, readType int) int64 {
	switch readType {
	case MemoryFirstThenStore:
	case ReadFromMemory:
		r.offsetTableLock.RLock()
		offset, ok := r.offsetTable[*mq]
		r.offsetTableLock.RUnlock()
		if ok {
			return offset
		} else if readType == ReadFromMemory {
			return -1
		}
	case ReadFromStore:
		offset, err := r.fetchConsumeOffsetFromBroker(mq)
		if err != nil {
			fmt.Println(err)
			return -1
		}
		r.updateOffset(mq, offset, false)
		return offset
	}

	return -1

}

func (r *RemoteOffsetStore) fetchConsumeOffsetFromBroker(mq *MessageQueue) (int64, error) {
	brokerAddr, _, found := r.mqClient.findBrokerAddressInSubscribe(mq.brokerName, 0, false)

	if !found {
		if _, err := r.mqClient.updateTopicRouteInfoFromNameServerKernel(mq.topic, false, DefaultProducer{}); err != nil {
			return 0, err
		}
		brokerAddr, _, found = r.mqClient.findBrokerAddressInSubscribe(mq.brokerName, 0, false)
	}

	if found {
		requestHeader := &QueryConsumerOffsetRequestHeader{}
		requestHeader.Topic = mq.topic
		requestHeader.QueueId = mq.queueId
		requestHeader.ConsumerGroup = r.groupName
		return r.mqClient.queryConsumerOffset(brokerAddr, requestHeader, 3000)
	}

	return 0, errors.New("fetch consumer offset error")
}

func (r *RemoteOffsetStore) persistAll(mqs []MessageQueue) {
	return
}

func (r *RemoteOffsetStore) persist(mq *MessageQueue) {
	offset, ok := r.offsetTable[*mq]
	if ok {
		err := r.updateConsumeOffsetToBroker(mq, offset)
		if err != nil {
			fmt.Println(err)
		}
	}
}

type UpdateConsumerOffsetRequestHeader struct {
	consumerGroup string
	topic         string
	queueId       int32
	commitOffset  int64
}

func (r *RemoteOffsetStore) updateConsumeOffsetToBroker(mq *MessageQueue, offset int64) error {
	addr, found, _ := r.mqClient.findBrokerAddressInSubscribe(mq.brokerName, 0, false)
	if !found {
		if _, err := r.mqClient.updateTopicRouteInfoFromNameServerKernel(mq.topic, false, DefaultProducer{}); err != nil {
			return err
		}
		addr, found, _ = r.mqClient.findBrokerAddressInSubscribe(mq.brokerName, 0, false)
	}

	if found {
		requestHeader := &UpdateConsumerOffsetRequestHeader{
			consumerGroup: r.groupName,
			topic:         mq.topic,
			queueId:       mq.queueId,
			commitOffset:  offset,
		}

		r.mqClient.updateConsumerOffsetOneway(addr, requestHeader, 5*1000)
		return nil
	}
	return errors.New("not found broker")
}

func (r *RemoteOffsetStore) updateOffset(mq *MessageQueue, offset int64, increaseOnly bool) {
	if mq != nil {
		r.offsetTableLock.RLock()
		offsetOld, ok := r.offsetTable[*mq]
		r.offsetTableLock.RUnlock()
		if !ok {
			r.offsetTableLock.Lock()
			r.offsetTable[*mq] = offset
			r.offsetTableLock.Unlock()
		} else {
			if increaseOnly {
				atomic.AddInt64(&offsetOld, offset)
				r.offsetTableLock.Lock()
				r.offsetTable[*mq] = offsetOld
				r.offsetTableLock.Unlock()
			} else {
				r.offsetTableLock.Lock()
				r.offsetTable[*mq] = offset
				r.offsetTableLock.Unlock()
			}
		}

	}

}

func (r *RemoteOffsetStore) OffsetTable() map[MessageQueue]int64 {
	return r.offsetTable
}

type LocalFileOffset struct {
	storePath       string
	groupName       string
	mqClient        *MqClient
	offsetTable     map[MessageQueue]int64
	offsetTableLock sync.RWMutex
}

func NewLocalFileOffset(mqClient *MqClient, groupName string, offsetTable map[MessageQueue]int64) *LocalFileOffset {
	offsetStore := &LocalFileOffset{
		groupName:   groupName,
		mqClient:    mqClient,
		offsetTable: offsetTable,
	}
	offsetStore.storePath = localOffsetStoreDir() + "/" +
		offsetStore.mqClient.clientId + "/" + offsetStore.groupName + "/" + "offset.json"

	return offsetStore
}

func localOffsetStoreDir() string {
	if dir := os.Getenv("ROCKETMQ_OFFSET"); dir != "" {
		return dir
	}
	home, err := home()
	if err != nil {
		return "/tmp/.rocketmq_offsets"
	}
	return home + "/.rocketmq_offsets"
}

func home() (string, error) {
	user, err := user.Current()
	if err == nil {
		return user.HomeDir, nil
	}
	if "windows" == runtime.GOOS {
		panic("discard")
	}
	if home := os.Getenv("HOME"); home != "" {
		return home, nil
	}
	var stdout bytes.Buffer
	cmd := exec.Command("sh", "-c", "eval echo ~$USER")
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return "", err
	}

	result := strings.TrimSpace(stdout.String())
	if result == "" {
		return "", errors.New("blank output when reading home directory")
	}

	return result, nil
}

type offsetWrapper struct {
	offsetTable map[MessageQueue]int64
}

func (l *LocalFileOffset) load() error {
	l.offsetTableLock.RLock()
	defer l.offsetTableLock.RUnlock()
	ow, err := l.readLocalOffset()
	if err != nil {
		return err
	}
	if ow != nil && len(ow.offsetTable) != 0 {
		for k, v := range ow.offsetTable {
			l.offsetTable[k] = v
		}
		for mq, offset := range ow.offsetTable {
			log.Printf("load consumer's offset, %s %s %s", l.groupName, mq, offset)
		}
	}

	return nil
}

func (l *LocalFileOffset) updateOffset(mq *MessageQueue, offset int64, increaseOnly bool) {
	if mq != nil {
		l.offsetTableLock.RLock()
		defer l.offsetTableLock.RUnlock()
		offsetOld, ok := l.offsetTable[*mq]
		if !ok {
			l.offsetTable[*mq] = offset
		} else {
			if increaseOnly {
				if offset > offsetOld {
					l.offsetTable[*mq] = offset
				}
			} else {
				l.offsetTable[*mq] = offset
			}
		}
	}
}

func (l *LocalFileOffset) readOffset(mq *MessageQueue, readType int) int64 {
	if mq != nil {
		switch readType {
		case MemoryFirstThenStore:
		case ReadFromMemory:
			l.offsetTableLock.RLock()
			offset, ok := l.offsetTable[*mq]
			l.offsetTableLock.RUnlock()
			if ok {
				return offset
			} else if readType == ReadFromMemory {
				return -1
			}
		case ReadFromStore:
			ow, err := l.readLocalOffset()
			if err != nil {
				fmt.Println(err)
				return -1
			}
			if ow != nil && len(ow.offsetTable) == 0 {
				offset, ok := ow.offsetTable[*mq]
				if ok {
					l.updateOffset(mq, offset, false)
					return offset
				}
			}
		}
	}
	return -1
}

func (l *LocalFileOffset) persistAll(mqs []MessageQueue) {
	if len(mqs) == 0 {
		return
	}
	ow := &offsetWrapper{offsetTable: make(map[MessageQueue]int64)}
	for _, mq := range mqs {
		offset, ok := l.offsetTable[mq]
		if ok {
			ow.offsetTable[mq] = offset
		}
	}
	bs := make([]byte, 0)
	err := json.Unmarshal(bs, ow)
	if err != nil {
		err := bytes2file(bs, l.storePath)
		if err != nil {
			log.Printf("persistAll consumer offset Exception, "+l.storePath, err)
		}
	}
}

func (l *LocalFileOffset) OffsetTable() map[MessageQueue]int64 {
	return l.offsetTable
}

func (l *LocalFileOffset) readLocalOffset() (*offsetWrapper, error) {
	ow := &offsetWrapper{}
	content, err := file2Bytes(l.storePath)
	if err != nil {
		log.Printf("Local local offset store file exception:%s", err)
	}
	if len(content) == 0 {
		return l.readLocalOffsetBak()
	} else {
		err := json.Unmarshal(content, ow)
		if err != nil {
			return l.readLocalOffsetBak()
		}
	}

	return ow, nil
}

func (l *LocalFileOffset) readLocalOffsetBak() (*offsetWrapper, error) {
	ow := &offsetWrapper{}
	content, err := file2Bytes(l.storePath)
	if err != nil {
		log.Printf("Load local offset store bak file exception:%s", err)
	}
	if len(content) == 0 {
		return ow, nil
	} else {
		err := json.Unmarshal(content, ow)
		if err != nil {
			return l.readLocalOffsetBak()
		}
	}
	return ow, nil
}

func file2Bytes(filepath string) ([]byte, error) {
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		return []byte{}, nil
	}
	return ioutil.ReadFile(filepath)

}

func bytes2file(bs []byte, filepath string) error {
	tmpfile := filepath + ".tmp"
	bakfile := filepath + ".bak"
	prev, _ := file2Bytes(filepath)
	if len(prev) == 0 {
		err := ioutil.WriteFile(bakfile, prev, 0777)
		if err != nil {
			return err
		}
	}
	err := ioutil.WriteFile(tmpfile, bs, 0777)
	if err != nil {
		return err
	}
	return os.Rename(tmpfile, filepath)
}
