package lsp

import (
	"context"
	"time"

	"github.com/cmu440/lspnet"
)

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
}

func (s *Scheduler) Cancel() {
	s.cancel()
}

func (s *Scheduler) Done() <-chan struct{} {
	return s.ctx.Done()
}

func (s *Scheduler) Tick() {
	s.Lost()
	s.send.Tick()
}

func (s *Scheduler) Ack(msg *Message) {
	s.send.Ack(msg)
	s.lastRecvTimeStamp = time.Now()
}

func (s *Scheduler) Send(payload []byte) {
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

func (s *Scheduler) RecvOutput() <-chan *Message {
	return s.recv.Output()
}

func (s *Scheduler) Lost() bool {
	if s.lost {
		return true
	}
	if time.Since(s.lastRecvTimeStamp).Milliseconds() > int64(s.params.EpochLimit)*int64(s.params.EpochMillis) {
		if s.send.unAckSn == 0 {
			s.lost = true
			return true
		}
	}
	return false
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
