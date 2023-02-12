package lsp

import (
	"context"
	"sync"
	"time"

	"github.com/cmu440/lspnet"
	log "github.com/sirupsen/logrus"
)

type State int

const (
	StateRuning = 0
	StateCloing = 1
	StateClosed = 2
)

type MessageWithErr struct {
	message *Message
	err     error
}
type Scheduler struct {
	addr              *lspnet.UDPAddr
	connId            int
	send              *SendScheduler
	recv              *RecvScheduler
	ctx               context.Context
	cancel            context.CancelFunc
	lastRecvTimeStamp time.Time
	params            *Params
	lost              bool
	sendMtx           sync.Mutex
	closing           bool
	closed            bool
	epoch             int
	state             State
}

func (s *Scheduler) Close() {
	s.sendMtx.Lock()
	defer s.sendMtx.Unlock()

	s.closing = true
}

func (s *Scheduler) Closing() bool {
	s.sendMtx.Lock()
	defer s.sendMtx.Unlock()

	return s.closing
}

func (s *Scheduler) Closed() bool {
	s.sendMtx.Lock()
	defer s.sendMtx.Unlock()

	return s.closed
}

func (s *Scheduler) Cancel() {
	s.cancel()
}

func (s *Scheduler) Done() <-chan struct{} {
	return s.ctx.Done()
}

func (s *Scheduler) State() State {
	s.sendMtx.Lock()
	defer s.sendMtx.Unlock()

	return s.state
}

func (s *Scheduler) Tick(e int) State {
	s.sendMtx.Lock()
	defer s.sendMtx.Unlock()

	s.epoch = e
	s.send.Tick(e)

	// check closing
	if s.closing {
		// all pending msg are sent and acked
		if s.send.unAckSn == 0 && s.send.sendList.Len() == 0 {
			s.closed = true
		}
	}

	if s.closed {
		s.state = StateClosed
		return StateClosed
	}

	if s.closing {
		s.state = StateCloing
		return StateCloing
	}

	return StateRuning
}

func (s *Scheduler) Ack(msg *Message) {
	s.sendMtx.Lock()
	defer s.sendMtx.Unlock()

	s.send.Ack(msg)
	s.lastRecvTimeStamp = time.Now()
}

func (s *Scheduler) Send(payload []byte) {
	s.sendMtx.Lock()
	defer s.sendMtx.Unlock()

	s.send.Send(payload)
}

func (s *Scheduler) Recv(msg *Message) {
	s.recv.Recv(msg)
	s.lastRecvTimeStamp = time.Now()
}

func (s *Scheduler) ConnID() int {
	return s.connId
}
func (s *Scheduler) AddrStr() string {
	return s.addr.String()
}

func (s *Scheduler) SendOutput() <-chan *Message {
	return s.send.Output()
}

func (s *Scheduler) RecvOutput() <-chan *MessageWithErr {
	return s.recv.Output()
}

func (s *Scheduler) Lost(check bool) bool {
	s.sendMtx.Lock()
	defer s.sendMtx.Unlock()

	if !check {
		return s.lost
	}

	if s.lost {
		return true
	}

	lost := false
	if time.Since(s.lastRecvTimeStamp).Milliseconds() > int64(s.params.EpochLimit)*int64(s.params.EpochMillis) {
		if s.send.unAckSn == 0 && s.send.sendList.Len() == 0 {
			lost = true
			log.WithField("connId", s.connId).WithField("lastRecvTimeStamp", s.lastRecvTimeStamp).WithField("now", time.Now()).Error("timeout, the conn is lost")
		}
	}

	s.lost = lost

	if lost {
		s.recv.EndWithAErr()
	}

	return lost
}

func NewScheduler(ctx context.Context, connId int, sn int, params *Params, addr *lspnet.UDPAddr) *Scheduler {
	newCtx, cancel := context.WithCancel(ctx)
	s := NewSendScheduler(newCtx, connId, sn, params)
	r := NewRecvScheduler(newCtx, connId, sn, params)

	c := &Scheduler{
		send:              s,
		recv:              r,
		ctx:               ctx,
		cancel:            cancel,
		connId:            connId,
		addr:              addr,
		lastRecvTimeStamp: time.Now(),
		params:            params,
		state:             StateRuning,
	}
	return c
}
