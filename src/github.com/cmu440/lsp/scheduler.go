package lsp

import (
	"context"
	"sync"
	"time"

	"github.com/cmu440/lspnet"
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
}

func (s *Scheduler) Cancel() {
	s.cancel()
}

func (s *Scheduler) Done() <-chan struct{} {
	return s.ctx.Done()
}

func (s *Scheduler) Tick() {
	s.sendMtx.Lock()
	defer s.sendMtx.Unlock()

	s.send.Tick()
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

func (s *Scheduler) Lost() bool {
	s.sendMtx.Lock()
	defer s.sendMtx.Unlock()

	if s.lost {
		return true
	}

	lost := false
	if time.Since(s.lastRecvTimeStamp).Milliseconds() > int64(s.params.EpochLimit)*int64(s.params.EpochMillis) {
		if s.send.unAckSn == 0 && s.send.sendList.Len() == 0 {
			lost = true
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
	}
	return c
}
