package world

import (
	"bufio"
	"fmt"
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

type Process struct {
	node    int
	wg      *sync.WaitGroup
	ch      chan Message
	workers []workFunction
}

func NewProcess(node int, wg *sync.WaitGroup) *Process {
	p := new(Process)
	p.node = node
	p.wg = wg
	p.ch = make(chan Message)
	p.workers = make([]workFunction)
}

func (p *Process) RegisterFunction(f workFunction) {
	p.workers = append(p.workers, f)
}

func (p *Process) Run() {
	go func(ch <-chan Message, wg *sync.WaitGroup, workers []workFunction) {
		defer wg.Done()
		for msg := range ch {
			for _, worker := range workers {
				if worker(msg) {
					break
				}
			}
		}
	}(p.ch, p.wg, p.workers)
}

type World struct {
	nl         *NetworkLayer
	wg         *sync.WaitGroup
	processes  []*Process
	associates map[string]workFunction
}

func NewWorld() *World {
	w := new(World)
	w.nl = NewNetworkLayer()
	w.wg = new(sync.WaitGroup)
	w.processes = make([]*Process)
	w.associates = make(map[string]workFunction)
	return w
}

func (w *World) Run() {
	for _, process := range w.processes {
		if process != nil {
			w.wg.Add(1)
			process.Run()
		}
	}
}

func (w *World) Wait() {
	for _, process := range w.processes {
		if process != nil {
			close(process.ch)
		}
	}
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
			w.nl.Send(from, to, NewMessageWithArg(msg, arg))
		} else if n, err := fmt.Sscanf(s, "send from %v to %v %v", &from, &to, &msg); err == nil && n == 3 {
			w.nl.Send(from, to, NewMessage(msg))
		} else if n, err := fmt.Sscanf(s, "wait %v", &timeout); err == nil && n == 1 {
			time.Sleep(timeout * time.Second)
		} else if n, err := fmt.Sscanf(s, "launch timer %v", &timer); err == nil && n == 1 {
			go timerSender(w.nl, timer)
		} else {
			fmt.Printf("unknown directive in input file: '%v'\n", s)
		}
	}
	return true
}
