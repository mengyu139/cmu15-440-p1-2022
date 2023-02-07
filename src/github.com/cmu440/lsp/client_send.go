package lsp

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// msg from sendMsgCh will be writen to UDP immediately
func (c *client) sendLoop() {
	defer c.g.Done()

	for {
		select {
		case <-c.ctx.Done():
			log.WithField("func", "sendLoop").Info("exit")
			return
		case msg := <-c.sendMsgCh:
			b, err := json.Marshal(msg)
			if err != nil {
				log.WithError(err).Error("marshal msg failed")
				continue
			}
			c.conn.Write(b)
		}
	}
}

// check window size and max unack size, put data into pool or push it into sendMsgCh
func (c *client) sendDataLoop() {
	defer c.g.Done()
	sentSn := 0

	for {
		// ack and write data
		select {
		case <-c.ctx.Done():
			log.WithField("func", "sendNewDataLoop").Info("exit")
			return

		case msg := <-c.sendAckCh:
			// mock ack msg to trigger send data
			if msg == nil {
				break
			}

			if msg.Type != MsgAck && msg.Type != MsgCAck {
				continue
			}

			isn := msg.SeqNum
			if isn == 0 {
				log.Error("SeqNum from sendAckCh is 0")
				continue
			}

			// not mark yet
			if c.windowMemo[isn] != nil {
				c.windowMemo[isn] = nil
				c.unAckCnt -= 1

				// update minAckSn
				minAcked := c.minAckSn
				for {
					if c.windowMemo[isn-1] == nil {
						minAcked = isn
						isn += 1
						continue
					}
					break
				}
				c.minAckSn = minAcked
			}

			// write ack to udp
			if err := c.sendMsg(msg); err != nil {
				return
			}

		}

		if c.unAckCnt >= c.params.MaxUnackedMessages {
			continue
		}
		if sentSn-c.minAckSn >= c.params.WindowSize {
			continue
		}

		select {
		case <-c.ctx.Done():
			log.WithField("func", "sendNewDataLoop").Info("exit")
			return

		case cmsg := <-c.sendDataCh:
			c.unAckCnt += 1
			c.windowMemo[cmsg.Message.SeqNum] = cmsg
			cmsg.firstTime = time.Now()
			cmsg.lastTime = cmsg.firstTime
			c.sendMsg(cmsg.Message)
			sentSn = cmsg.Message.SeqNum
		}

	}

}

// check backoff
func (c *client) sendBackoffLoop() {
	defer c.g.Done()
	select {
	case <-c.ctx.Done():
		log.WithField("func", "sendBackoffLoop").Info("exit")
		return

	// backoff
	case <-c.epochTimer.C:
		// check client lost
		if c.freeCnt >= c.params.EpochLimit {
			c.cancel()
			err := fmt.Errorf("wait too long before connect")
			log.WithError(err).Error("connect failed")
			return
		}

		// check backoff
		min := c.minAckSn
		for i := 0; i < c.params.WindowSize; i++ {
			index := i + min
			if v, ok := c.windowMemo[index]; ok {
				// already acked
				if v == nil {
					continue
				}
				if v.timeout {
					continue
				}

				if time.Since(v.lastTime).Milliseconds() < int64(c.params.EpochMillis)*int64(v.curBackoff) {
					continue
				}

				v.lastTime = time.Now()

				if v.curBackoff == 0 {
					v.curBackoff = 1
				} else {
					v.curBackoff *= 2
				}

				if v.curBackoff > c.params.MaxBackOffInterval {
					v.timeout = true
					continue
				}

				c.sendMsg(v.Message)
			}
		}

	}
}

// ack from server
// data to send to server
func (c *client) sendData(msg *MessageData) error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	case c.sendDataCh <- msg:
		return nil
	}
}

func (c *client) sendAck(msg *Message) error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	case c.sendAckCh <- msg:
		return nil
	}
}

func (c *client) sendMsg(msg *Message) error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	case c.sendMsgCh <- msg:
		return nil
	}
}
