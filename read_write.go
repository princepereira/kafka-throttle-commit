package main

import (
	"confluent-kafka-go/kafka"
	"encoding/json"
	"errors"
	"fmt"
	"kaf-client/client"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const msgCount = 1000
const topic = "DEVICEMGR"
const manualCommit = false
const orderedMsg = false
const skipMsg = true

var maxCommitWait int64 = 10
var commitMap = make(map[kafka.Offset]*CommitInfo)
var offsetBegin kafka.Offset = -1
var offsetEnd kafka.Offset = -1
var mutex = new(sync.Mutex)

var counter int

type Msg struct {
	Name string
}

var msgEnd = make(chan bool)

func main() {
	startProducer()
	time.Sleep(3 * time.Second)
	startTime := time.Now().Unix()
	go startConsumer()

	<-msgEnd

	endTime := time.Now().Unix()
	fmt.Printf("#------------- Manual Commit : %v , Ordered Msgs : %v -------- #\n", manualCommit, orderedMsg)
	fmt.Printf("#------------- Time taken to complete %d messages : %v seconds -------- #\n", msgCount, (endTime - startTime))
}

func writeMsg(topic, x string, handler client.MsgWriter) {
	fmt.Println("#======== WRITER Writing message ", x, " ========#")
	health := new(Msg)
	health.DstTopic = topic
	health.Name = x
	handler.WriteMsg(health)
	time.Sleep(1 * time.Millisecond)
}

func startProducer() {
	fmt.Printf("#======== WRITER started ======== #\n")
	handler := client.Writer()
	if handler == nil {
		log.Println("Init Failed")
		os.Exit(1)
	}
	err := handler.Connect()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	for i := 0; i < msgCount; i++ {
		writeMsg("DEVICEMGR", strconv.Itoa(i), handler)
	}
	handler.Disconnect()
	fmt.Printf("#======== WRITER completed ======== #\n")
}

// CommitInfo abc
type CommitInfo struct {
	ReceivedTime    int64
	OffsetCommitted bool
	Msg             *kafka.Message
}

func printMapKeys() []kafka.Offset {
	var keys []kafka.Offset
	for k := range commitMap {
		keys = append(keys, k)
	}
	return keys
}

func msgReceived(msg *kafka.Message) {
	mutex.Lock()
	defer mutex.Unlock()
	commitInfo := new(CommitInfo)
	commitInfo.ReceivedTime = time.Now().Unix()
	commitInfo.Msg = msg
	commitMap[msg.TopicPartition.Offset] = commitInfo
	if offsetBegin == -1 {
		offsetBegin = msg.TopicPartition.Offset
	}
	offsetEnd = msg.TopicPartition.Offset
}

func commitOffset(msg *kafka.Message, c *kafka.Consumer) error {
	mutex.Lock()
	defer mutex.Unlock()

	fmt.Printf("#========= READER - Offset range before CommitOffset : %v - %v Map : %v ========= #\n", offsetBegin, offsetEnd, printMapKeys())
	fmt.Println("")
	if info, ok := commitMap[msg.TopicPartition.Offset]; ok {
		info.OffsetCommitted = true
	}

	var lastCommittedMsg *kafka.Message
	// lastCommitIdx := -1

	if offsetBegin == msg.TopicPartition.Offset {

		for i := offsetBegin; i <= offsetEnd; i++ {
			if info, ok := commitMap[i]; ok && info.OffsetCommitted {
				lastCommittedMsg = info.Msg
				delete(commitMap, i)
				offsetBegin = i + 1
			} else {
				break
			}
		}

		c.CommitMessage(lastCommittedMsg)
		fmt.Printf("#========= READER - Committing-batch-msgs. Offset committed : %v ========= #\n", lastCommittedMsg.TopicPartition.Offset)
		fmt.Println("")
	}

	if offsetBegin > offsetEnd {
		offsetBegin = -1
		fmt.Printf("#========= READER - Offset range after CommitOffset : %v - %v - Map : %v ========= #\n", offsetBegin, offsetEnd, printMapKeys())
		fmt.Println("")
		return nil
	}

	timeNow := time.Now().Unix()
	lastCommittedMsg = nil

	// Commit long waiting messages
	for i := offsetBegin; i <= offsetEnd; i++ {
		if info, ok := commitMap[i]; ok {

			timeDiff := timeNow - info.ReceivedTime

			if timeDiff > maxCommitWait || info.OffsetCommitted {
				lastCommittedMsg = info.Msg
				delete(commitMap, i)
				offsetBegin = i + 1
			} else {
				break
			}

		}
	}

	if offsetBegin > offsetEnd {
		offsetBegin = -1
	}

	if lastCommittedMsg != nil {
		c.CommitMessage(lastCommittedMsg)
		fmt.Printf("#========= READER - Max time execeeded, Committing-long-waiting-msgs. Offset committed : %v ========= #\n", lastCommittedMsg.TopicPartition.Offset)
		fmt.Printf("#========= READER - Offset range after CommitOffset : %v - %v - Map : %v ========= #\n", offsetBegin, offsetEnd, printMapKeys())
		return errors.New("Long pending messages committed without offset")
	}

	fmt.Printf("#========= READER - Offset range after CommitOffset : %v - %v - Map : %v ========= #\n", offsetBegin, offsetEnd, printMapKeys())
	return nil
}

func commitLater(msg *kafka.Message, c *kafka.Consumer) {
	time.Sleep(1 * time.Second)
	commitNow(msg, c)
}

func commitNow(msg *kafka.Message, c *kafka.Consumer) {
	fmt.Printf("#========= READER Commit called for Offset : %v =========#\n", msg.TopicPartition.Offset)
	if skipMsg {
		time.Sleep(1 * time.Second)
	}
	if manualCommit {
		c.CommitMessage(msg)
	} else {
		commitOffset(msg, c)
	}
	counter = counter + 1
	if counter == msgCount {
		fmt.Println("#============ ", counter)
		msgEnd <- true
	}
}

func process(msg *kafka.Message, c *kafka.Consumer) {
	var structMsg = new(Msg)
	json.Unmarshal(msg.Value, structMsg)

	fmt.Printf("#========= READER Msg : %s , Offset : %v =========#\n", structMsg.PeerID, msg.TopicPartition.Offset)
	if orderedMsg {
		commitNow(msg, c)
	} else {
		val, _ := strconv.Atoi(structMsg.PeerID)
		if skipMsg && val%100 == 0 {
			fmt.Printf("#========= READER Skipping - Msg : %s , Offset : %v =========#\n", structMsg.PeerID, msg.TopicPartition.Offset)
		} else if val%5 == 0 {
			go commitLater(msg, c)
		} else {
			commitNow(msg, c)
		}
	}
}

func readMsgs(c *kafka.Consumer) {
	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("#========= READER Msg Received time : %v , Offset : %v =========#\n", time.Now(), msg.TopicPartition.Offset)
			msgReceived(msg)
			process(msg, c)
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

func startConsumer() {
	fmt.Printf("#========= READER started ========= #\n")
	fmt.Println("")
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost",
		"group.id":           "myGroup",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		// "auto.commit.interval.ms": 10000,
		"enable.partition.eof": true,
	})
	if err != nil {
		panic(err)
	}
	c.SubscribeTopics([]string{topic}, nil)
	readMsgs(c)
	c.Close()
	fmt.Printf("#========= READER completed ========= #\n")
}
