package world

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"time"
	"sync"
	"github.com/seehuhn/mt19937"
)

type ErrorCode int32

const (
	OK ErrorCode = iota
	NotExpectedType
	ReadAttemptOutOfBounds
	ObjectIsNULL
	ResourceNotFound
	ResourceInUse
	PrematureEndOfStream
	SizeTooBig
	YetNotImplemented
	DuplicateItems
	ItemNotFound
	QueueIsEmpty
	SocketError
	ConnectionFailed
	ConnectionInUse
	TimeOut
)

type MessageType byte

const (
	Int32Type MessageType = "A" + iota
	Int64Type
	StringType
	BadType
)

const order = binary.LittleEndian

type MessageArg bytes.Buffer

func getMessageType(data interface{}) (MessageType, error) {
	switch data.(type) {
	case int32, *int32:
		return Int32Type, nil
	case int64, *int64:
		return Int64Type, nil
	case string, *string:
		return StringType, nil
	default:
		return BadType, errors.New("Expected data of type int32, int64 or string")
	}
}

func NewMessageArg(data interface{}) (*MessageArg, error) {
	ma := new(MessageArg)
	t, err := getMessageType(data)
	if err != nil {
		return nil, err
	}

	err = ma.WriteByte(t)
	if err != nil {
		return nil, err
	}

	err = binary.Write(ma, order, data)
	if err != nil {
		return nil, err
	}

	return ma, nil
}

type Message struct {
	sendTime, deliveryTime int64
	from, to               int32
	body                   bytes.Buffer
}

func (m *Message) Append(ma *MessageArg) error {
	_, err := io.Copy(m.body, ma)
	return err
}

func (m *Message) getMessage(data interface{}) error {
	t, err := getMessageType(data)
	if err != nil {
		return err
	}

	b, err := m.body.ReadByte()
	if err != nil {
		return 0, err
	} else if b != t {
		m.body.UnreadByte()
		text = fmt.Sprintf("Expected %T", data)
		return 0, errors.New(text)
	}

	if t == StringType {
		data = m.body.String()
		return nil
	}

	return bytes.Read(m.body, order, data)
}

func (m *Message) GetInt32() (int32, error) {
	var res int32
	err := getMessage(&res)
	return res, err
}

func (m *Message) GetInt64() (int64, error) {
	var res int64
	err := getMessage(&res)
	return res, err
}

func (m *Message) GetString() string {
	var res string
	err := getMessage(&res)
	return res, err
}

type MessageQueue []*Message

func (q MessageQueue) Len() int {
	return len(q)
}

func (q MessageQueue) Less(i, j int) bool {
	return q[i].value.deliveryTime > q[j].value.deliveryTime
}

func (q MessageQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *MessageQueue) Push(x interface{}) {
	*q = append(*q, x.(*Message))
}

func (q *MessageQueue) Pop() interface{} {
	old := *q
	n := len(old)
	msg := old[n-1]
	*q = old[0 : n-1]
	return msg
}

func (q *MessageQueue) Peek() interface{} {
	return q[len(q)-1]
}

type Process

type NetworkLayer struct {
	queues []*MessageQueue
	errorRate float64
	rng rand.Rand
	tick int64
	stopFlag bool
	networkSize int
	networkMap map[int]map[int]int
	mutex sync.Mutex
	wait chan int
}

func NewNetworkLayer() *NetworkLayer {
	nl := new(NetworkLayer)
	nl.wait = make(chan int, 1)
	nl.rng = rand.New(mt19937.New())
	rng.Seed(time.Now().UnixNano())

	go func() {
		start := time.Now()
		for !nl.stopFlag {
			nl.mutex.Lock()
			cl := time.Now().Sub(start)
			nl.tick = int64(cl.Seconds())
			nl.mutex.Unlock()

			time.Sleep(100 * time.Millisecond)
		}
		nl.wait <- 0
	}()
}

func (nl *NetworkLayer) Stop() {
	nl.mutex.Lock()
	nl.stopFlag = true
	nl.mutex.Unlock()
	<-nl.wait
}

func (nl *NetworkLayer) CreateLink(from, to int, bidirectional bool, cost int) {
	if from == to {
		return
	}
	networkMap[from][to] = cost
	if bidirectional {
		networkMap[to][from] = cost
	}
}

func (nl *NetworkLayer) GetLink(p1, p2 int) int {
	if p1 < 0 || p1 == p2 {
		return 0
	}
	if v, ok := nl.networkMap[p1]; ok {
		if p, ok := v[p2]; ok {
			return p
		}
	}
	return -1
}

func (nl *NetworkLayer) Send(fromProcess, toProcess int, msg *Message) int {
	if toProcess >= 0 {
		return send(fromProcess, toProcess, msg.body)
	}
	for i := range nl.queues {
		send(fromProcess, i, msg.body)
	}
	return OK
}

type workFunction func(*Process, Message) int32

type Process struct {}

type World struct {
	processes  []*Process
	associates map[string]workFunction
	nl         NetworkLayer
}

func (w *World) createProcess(node int32) int32 {
	p := new(Process)
	append(w.processes, p)
	nl.registerProcess(node, p)
	return node
}
