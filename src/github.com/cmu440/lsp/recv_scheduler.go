package lsp

import (
	"context"

	log "github.com/sirupsen/logrus"
)

type RecvScheduler struct {
	dataBuffer chan *Message

	// key: sn
	dataPool map[int]*Message

	nextRecvSn int
	params     *Params
	ctx        context.Context
	connId     int
	outCh      chan *Message
}

func NewRecvScheduler(ctx context.Context, connId int, sn int, params *Params) *RecvScheduler {
	r := &RecvScheduler{
		nextRecvSn: sn + 1,
		outCh:      make(chan *Message, 1000),
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
func (s *RecvScheduler) Output() <-chan *Message {
	return s.outCh
}

func (c *RecvScheduler) pushIntoBuffer(msg *Message) error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	case c.dataBuffer <- msg:
		return nil
	}
}
