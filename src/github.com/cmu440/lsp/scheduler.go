package lsp

import (
	"context"

	"github.com/cmu440/lspnet"
)

type Scheduler struct {
	addr   *lspnet.UDPAddr
	connId int
	send   *SendScheduler
	recv   *RecvScheduler
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *Scheduler) Cancel() {
	s.cancel()
}

func (s *Scheduler) Done() <-chan struct{} {
	return s.ctx.Done()
}

func (s *Scheduler) Tick() {
	s.send.Tick()
}

func (s *Scheduler) Ack(msg *Message) {
	s.send.Ack(msg)
}

func (s *Scheduler) Send(payload []byte) {
	s.send.Send(payload)
}

func (s *Scheduler) Recv(msg *Message) {
	s.recv.Recv(msg)
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

func NewScheduler(ctx context.Context, connId int, sn int, params *Params, addr *lspnet.UDPAddr) *Scheduler {
	newCtx, cancel := context.WithCancel(ctx)
	s := NewSendScheduler(newCtx, connId, sn, params)
	r := NewRecvScheduler(newCtx, connId, sn, params)

	c := &Scheduler{
		send:   s,
		recv:   r,
		ctx:    ctx,
		cancel: cancel,
		connId: connId,
		addr:   addr,
	}
	return c
}
