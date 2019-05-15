package world

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/seehuhn/mt19937"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"
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

const Order = binary.LittleEndian

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

func NewMessage(from, to int32) *Message {
	m := new(Message)
	m.from = from
	m.to = to
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
	return q[i].deliveryTime < q[j].deliveryTime
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

func (q MessageQueue) Peek() *Message {
	return q[0]
}

type NetworkLayer struct {
	queue      *MessageQueue
	errorRate  float64
	rng        rand.Rand
	networkMap map[int]map[int]int
	messages   chan *Message
}

func NewNetworkLayer() *NetworkLayer {
	nl := new(NetworkLayer)
	nl.rng = rand.New(mt19937.New())
	nl.rng.Seed(time.Now().UnixNano())
	nl.networkMap = make(map[int]map[int]int)
	nl.messages = make(chan *Message, 10)
}

func (nl NetworkLayer) Run(w World) {
	defer w.wg.Done()
	start := time.Now()
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	for {
		tick := time.Now().Sub(start).Seconds()
		var m *Message
		for nl.queue.Len() > 0 {
			m = nl.queue.Peek()
			if m.deliveryTime > tick {
				break
			}

			select {
			case w.processes[m.to].ch <- m:
				heap.Pop(nl.queue)
				continue
			case <-timer.C:
				break
			}
		}

		select {
		case msg <- nl.messages:
			nl.Enqueue(msg, &w, tick)
		case <-timer.C:
			continue
		case <-w.done:
			return
		}
	}
}

func (nl *NetworkLayer) CreateLink(from, to int, bidirectional bool, cost int) {
	if from == to {
		return
	}

	if _, ok := nl.networkMap[from]; !ok {
		nl.networkMap[from] = make(map[int]int)
	}
	networkMap[from][to] = cost

	if bidirectional {
		nl.CreateLink(to, from, false, cost)
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

func (nl *NetworkLayer) Enqueue(msg *Message, w *World, sendTime int64) {
	if msg.to >= 0 {
		if v, ok := nl.networkMap[msg.from]; ok {
			if c, ok := v[msg.to]; ok {
				msg.sendTime = sendTime
				msg.deliveryTime = sendTime + c
				heap.Push(nl.queue, msg)
			}
		}
	} else {
		for i := range w.processes {
			msg.to = i
			nl.Enqueue(msg, w, sendTime)
		}
	}
}

func (nl *NetworkLayer) GetNeibs(node int) []int {
	net, ok = nl.networkMap[node]
	if !ok {
		return nil
	}

	neibs := make([]int, len(net))
	i := 0
	for k := range net {
		neibs[i] = k
		i += 1
	}
	return neibs
}

type workFunction func(Message, *Process, *World) bool

type Process struct {
	node    int
	ch      chan *Message
	workers []workFunction
}

func NewProcess(node int, wg *sync.WaitGroup) *Process {
	p := new(Process)
	p.node = node
	p.wg = wg
	p.ch = make(chan *Message)
	p.workers = make([]workFunction)
}

func (p *Process) RegisterFunction(f workFunction) {
	p.workers = append(p.workers, f)
}

func (p *Process) IsMyMessage(prefix, msg string) bool {
	if len(msg) > 0 && msg[0] == '*' {
		return true
	}
	if len(prefix)+1 >= len(msg) {
		return false
	}
	for i, ch := range prefix {
		if msg[i] != ch {
			return false
		}
	}
	return msg[len(prefix)] == '_'
}

func (p Process) Run(w World) {
	defer w.wg.Done()
	for {
		select {
		case msg <- p.ch:
			for _, worker := range p.workers {
				if worker(w, &p, msg) {
					break
				}
			}
		case <-w.done:
			return
		}
	}
}

type World struct {
	nl         *NetworkLayer
	wg         *sync.WaitGroup
	processes  []*Process
	associates map[string]workFunction
	done       chan struct{}
}

func timerSender(w World, d int32) {
	defer w.wg.Done()
	current := 0
	t := time.NewTimer(d * time.Second)
	for <-t.C {
		current += 1
		m := NewMessage(-1, -1)
		m.Append(NewMessageArg("*TIME"))
		m.Append(NewMessageArg(current))
		select {
		case w.nl.messages <- m:
			continue
		case <-w.done:
			return
		}
	}
}

func NewWorld() *World {
	w := new(World)
	w.nl = NewNetworkLayer()
	w.wg = new(sync.WaitGroup)
	w.processes = make([]*Process)
	w.associates = make(map[string]workFunction)
	w.done = make(chan struct{})
	return w
}

func (w *World) Run() {
	for _, process := range w.processes {
		if process != nil {
			w.wg.Add(1)
			go process.Run(*w)
		}
	}
	w.wg.Add(1)
	go w.nl.Run(*w)
}

func (w *World) Stop() {
	close(w.done)
	w.wg.Wait()
}

func (w *World) CreateProcess(node int, f string) {
	if node >= len(w.processes) {
		w.processes = append(w.processes, make([]*Process, len(w.processes)-node+1)...)
	}
	w.processes[node] = NewProcess(node, w.wg)
	w.nl.RegisterProcess(node, w.processes[node])
}

func (w *World) AssignWorkFunction(node int, f string) ErrorCode {
	if node < 0 || node >= len(w.processes) {
		return ItemNotFound
	}
	p := w.processes[node]
	if p == nil {
		return ItemNotFound
	}
	a, ok := w.associates[f]
	if !ok {
		return ItemNotFound
	}
	p.RegisterFunction(f, a)
	return OK
}

func (w *World) RegisterWorkFunction(f string, wf workFunction) {
	w.associates[f] = wf
}

func (w *World) ParseConfig(n string) bool {
	f, err := os.Open(n)
	if err != nil {
		return false
	}
	defer f.Close()

	scanner = bufio.NewScanner(f)
	bidirected, timeout := 1, 0
	for scanner.Scan() {
		s := scanner.Text()
		if len(s) == 0 || s[0] == ';' {
			continue
		}

		var id, msg string
		var errorRate float64
		var startprocess, endprocess, from, to, arg int
		var latency, timer int = 1, 0
		if n, err := fmt.Sscanf(s, "bidirected %v", &bidirected); err == nil && n == 1 {
			continue
		} else if n, err := fmt.Sscanf(s, "errorRate %v", &errorRate); err == nil && n == 1 {
			w.nl.SetErrorRate(errorRate)
		} else if n, err := fmt.Sscanf(s, "processes %v %v", &startprocess, &endprocess); err == nil && n == 2 {
			for i := startprocess; i <= endprocess; i += 1 {
				w.CreateProcess(i)
			}
		} else if n, err := fmt.Sscanf(s, "link from %v to %v latency %v", &from, &to, &latency); err == nil && n == 3 {
			w.nl.CreateLink(from, to, bidirected != 0, latency)
		} else if n, err := fmt.Sscanf(s, "link from %v to %v", &from, &to); err == nil && n == 2 {
			w.nl.CreateLink(from, to, bidirected != 0, latency)
		} else if n, err := fmt.Sscanf(s, "link from %v to all latency %v", &from, &latency); err == nil && n == 2 {
			w.nl.CreateLinkToAll(from, bidirected != 0, latency)
		} else if n, err := fmt.Sscanf(s, "link from %v to all", &from); err == nil && n == 1 {
			w.nl.CreateLinkToAll(from, bidirected != 0, latency)
		} else if n, err := fmt.Sscanf(s, "link from all to %v latency %v", &to, &latency); err == nil && n == 2 {
			w.nl.CreateLinkFromAll(to, bidirected != 0, latency)
		} else if n, err := fmt.Sscanf(s, "link from all to %v", &to); err == nil && n == 1 {
			w.nl.CreateLinkFromAll(to, bidirected != 0, latency)
		} else if n, err := fmt.Sscanf(s, "setprocesses %v %v %v", &startprocess, &endprocess, &id); err == nil && n == 3 {
			for i := startprocess; i <= endprocess; i += 1 {
				w.AssignWorkFunction(i, id)
			}
		} else if n, err := fmt.Sscanf(s, "send from %v to %v %v %v", &from, &to, &msg, &arg); err == nil && n == 4 {
			m := NewMessage(from, to)
			m.Append(NewMessageArg(msg))
			m.Append(NewMessageArg(arg))
			w.nl.Enqueue(m, w, 0)
		} else if n, err := fmt.Sscanf(s, "send from %v to %v %v", &from, &to, &msg); err == nil && n == 3 {
			m := NewMessage(from, to)
			m.Append(NewMessageArg(msg))
			w.nl.Enqueue(m, w, 0)
		} else if n, err := fmt.Sscanf(s, "wait %v", &timeout); err == nil && n == 1 {
			time.Sleep(timeout * time.Second)
		} else if n, err := fmt.Sscanf(s, "launch timer %v", &timer); err == nil && n == 1 {
			w.wg.Add(1)
			go timerSender(*w, timer)
		} else {
			fmt.Printf("unknown directive in input file: '%v'\n", s)
		}
	}
	return true
}
