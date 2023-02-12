package lsp

import (
	"container/list"
	"context"
	"time"

	log "github.com/sirupsen/logrus"
)

type SendScheduler struct {
	connId int

	windowMemo map[int]*MessageData
	nextSendSn int
	nextRecvSn int

	minAckSn int
	unAckSn  int
	params   *Params

	outCh    chan *Message
	ctx      context.Context
	sendList *list.List

	epoch            int
	lastSendTimStamp time.Time
}

func NewSendScheduler(ctx context.Context, connId int, sn int, params *Params) *SendScheduler {
	r := &SendScheduler{
		connId:     connId,
		windowMemo: make(map[int]*MessageData),
		params:     params,
		nextSendSn: sn + 1,
		nextRecvSn: sn + 1,
		minAckSn:   sn,
		unAckSn:    0,
		ctx:        ctx,
		outCh:      make(chan *Message, 1000),
		sendList:   list.New(),
	}

	r.windowMemo[sn] = nil

	return r
}

// timer
// make backoff retry, retry msg will be send into outch
// no transfer will be make
func (s *SendScheduler) Tick(e int) {
	s.epoch = e

	for i := 0; i < s.params.WindowSize; i++ {
		if s.unAckSn == 0 {
			break
		}

		index := s.minAckSn + 1 + i
		v, ok := s.windowMemo[index]
		if !ok {
			log.WithField("sn", index).Info("not found in window")
			break
		}
		if v == nil {
			log.WithField("sn", index).Info("is nil")
			continue
		}
		if v.timeout {
			// todo: we need exit? or just skip it
			continue
		}

		t := time.Since(v.lastTime).Milliseconds()
		target := int64(s.params.EpochMillis) * int64(v.curBackoff+1)
		logger := log.WithField("cur milli", t).WithField("last time", v.lastTime).WithField("curBackoff", v.curBackoff).WithField("target", target)

		if t < target {
			logger.Debug("time too close, continue, no retry")
			continue
		}

		if v.curBackoff == 0 {
			v.curBackoff = 1
		} else {
			v.curBackoff *= 2
		}
		if v.curBackoff > s.params.MaxBackOffInterval {
			v.timeout = true
			logger.Error("timeout")
			continue
		}

		//resend
		v.lastTime = time.Now()
		log.WithField("sn", v.Message.SeqNum).Info("resend")
		s.output(v.Message)
	}

	// heartbeat
	if time.Since(s.lastSendTimStamp).Milliseconds() > int64(s.params.EpochMillis)/2 {
		s.output(NewAck(s.connId, 0))
	}

}

// ack
// update window and unack cnt
// transfer will be make
func (s *SendScheduler) Ack(msg *Message) {
	// filter
	if msg.Type != MsgAck && msg.Type != MsgCAck {
		return
	}
	// heartbeat
	if msg.ConnID == 0 || msg.SeqNum == 0 {
		return
	}

	// update window
	sn := msg.SeqNum
	if sn <= s.minAckSn {
		return
	}

	v, ok := s.windowMemo[sn]
	if ok && v != nil {
		s.windowMemo[sn] = nil
		s.unAckSn -= 1
	}

	min := s.minAckSn
	for i := 0; i < s.params.WindowSize; i++ {
		index := i + min + 1
		v, ok := s.windowMemo[index]
		if !ok {
			break
		}
		if v != nil {
			break
		}
		s.minAckSn = index
	}

	s.transfer()
}

// send request
// data sent by client will be firstly add to list as buffer
// transfer will be make
func (s *SendScheduler) Send(payload []byte) {
	sn := s.assignSn()

	// put msg into list
	msg := NewData(s.connId, sn, len(payload), payload, CalculateChecksum(s.connId, sn, len(payload), payload))
	s.sendList.PushBack(msg)

	// and then, transfer msg from list to chan, of window allows
	s.transfer()
}

// send request
func (s *SendScheduler) Output() <-chan *Message {
	return s.outCh
}

func (s *SendScheduler) transfer() {
	if s.sendList.Len() == 0 {
		return
	}

	elm := s.sendList.Front()
	msg := elm.Value.(*Message)

	// check
	if msg.SeqNum-s.minAckSn > s.params.WindowSize {
		return
	}
	if s.unAckSn >= s.params.MaxUnackedMessages {
		return
	}

	s.unAckSn += 1
	s.windowMemo[msg.SeqNum] = CreateMessageData(msg)

	s.output(msg)

	// remove it!
	s.sendList.Remove(elm)
	log.WithField("unAckSn", s.unAckSn).WithField("pending len", s.sendList.Len()).Info("window")
}

func (s *SendScheduler) output(msg *Message) {
	select {
	case <-s.ctx.Done():
		return
	case s.outCh <- msg:
		s.lastSendTimStamp = time.Now()
		return
	}
}

func (s *SendScheduler) assignSn() int {
	sn := s.nextSendSn
	s.nextSendSn += 1
	return sn
}
