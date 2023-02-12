package lsp

import (
	"context"

	log "github.com/sirupsen/logrus"
)

type RecvScheduler struct {
	dataBuffer chan *MessageWithErr

	// key: sn
	dataPool map[int]*Message

	nextRecvSn int
	params     *Params
	ctx        context.Context
	connId     int
}

func NewRecvScheduler(ctx context.Context, connId int, sn int, params *Params) *RecvScheduler {
	r := &RecvScheduler{
		nextRecvSn: sn + 1,
		dataBuffer: make(chan *MessageWithErr, 1000),
		connId:     connId,
		ctx:        ctx,
		params:     params,
	}

	return r
}

func (c *RecvScheduler) Recv(msg *Message) {
	if msg.SeqNum < c.nextRecvSn {
		log.WithField("nextRecvSn", c.nextRecvSn).WithField("msg SeqNum", msg.SeqNum).Info("skip processed data msg")
		return
	}

	if msg.SeqNum != c.nextRecvSn {
		c.dataPool[msg.SeqNum] = msg
	} else {
		c.pushIntoBuffer(msg)
		c.nextRecvSn += 1
	}

	for {
		data, ok := c.dataPool[c.nextRecvSn]
		if !ok {
			break
		}

		delete(c.dataPool, c.nextRecvSn)
		c.pushIntoBuffer(data)
		c.nextRecvSn += 1
	}
}

// retrive data msg
func (s *RecvScheduler) Output() <-chan *MessageWithErr {
	return s.dataBuffer
}

func (c *RecvScheduler) EndWithAErr() {
	select {
	case <-c.ctx.Done():
		return
	case c.dataBuffer <- &MessageWithErr{message: &Message{ConnID: c.connId}, err: errClientLost}:
		return
	}
}

func (c *RecvScheduler) pushIntoBuffer(msg *Message) error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	case c.dataBuffer <- &MessageWithErr{message: msg}:
		return nil
	}
}
