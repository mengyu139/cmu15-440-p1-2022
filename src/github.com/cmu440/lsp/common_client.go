package lsp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cmu440/lspnet"
	log "github.com/sirupsen/logrus"
)

type CommonClient struct {
	// control
	ctx    context.Context
	cancel context.CancelFunc
	g      *sync.WaitGroup

	epochTimer *time.Timer

	params        *Params
	conn          *lspnet.UDPConn
	connectedConn bool
	remoteAddr    *lspnet.UDPAddr
	connID        int

	sendCh     chan *Message
	writeCh    chan *Message
	ackCh      chan *Message
	dataCh     chan *Message
	done       chan int
	dataBuffer chan []byte

	epoch                   int
	notIdleSendForLastEpoch bool // there is send action last epoch
	lastRecvEpoch           int  // lastest epoch stamp when recieve msg from server

	minAckSn   int
	windowMemo map[int]*MessageData

	nextSendSn int
}

// if the client is create by Client end, then the conn is connected conn, other than is unconnected conn for server end
func NewCommonClient(ctx context.Context, connID int, params *Params, sn int, remoteAddr *lspnet.UDPAddr) *CommonClient {
	ctx, cancel := context.WithCancel(ctx)

	connected := true
	if remoteAddr != nil {
		connected = false
	}

	c := &CommonClient{
		ctx:    ctx,
		cancel: cancel,
		params: params,
		connID: connID,

		epochTimer: time.NewTimer(time.Millisecond * time.Duration(params.EpochMillis)),

		ackCh:         make(chan *Message, params.WindowSize*2+1),
		dataCh:        make(chan *Message, 100),
		sendCh:        make(chan *Message, params.WindowSize*2+1),
		dataBuffer:    make(chan []byte, 1),
		g:             &sync.WaitGroup{},
		connectedConn: connected,
		remoteAddr:    remoteAddr,
		done:          make(chan int),
	}
	for i := 0; i < params.MaxUnackedMessages; i++ {
		c.ackCh <- nil
	}

	c.g.Add(2)
	go c.mainLoop()
	go c.writeLoop()

	// client end
	if c.connectedConn {
		c.g.Add(1)
		go c.recvLoop()
	}

	return c
}

func (c *CommonClient) Send(payload []byte) error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	default:
	}

	// check := CalculateChecksum(c.connID, c.nextSendSn, len(payload), payload)
	msg := NewData(c.connID, c.nextSendSn, len(payload), payload, 0)
	c.nextSendSn += 1

	return c.pushIntoCh(c.sendCh, msg)
}

func (c *CommonClient) Read() ([]byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, errClientClosed
	case data, ok := <-c.dataBuffer:
		if !ok {
			err := fmt.Errorf("dataBuffer is closed")
			return nil, err
		}
		return data, nil
	}
}

func (c *CommonClient) AddrStr() string {
	return c.remoteAddr.String()
}

func (c *CommonClient) Recv(msg *Message) error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	default:
		if msg.Type == MsgAck || msg.Type == MsgCAck {
			return c.RecvAck(msg)
		} else if msg.Type == MsgData {
			c.RecvData(msg)
		}
		return nil
	}
}

func (c *CommonClient) RecvAck(msg *Message) error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	case c.ackCh <- msg:
		return nil
	}
}

func (c *CommonClient) RecvData(msg *Message) error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	case c.dataCh <- msg:
		return nil
	}
}

func (c *CommonClient) Closed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *CommonClient) Close() error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	default:
		c.cancel()
		c.g.Wait()
		close(c.done)
	}

	return nil
}

func (c *CommonClient) mainLoop() {
	defer log.Info("exit mainLoop")

	for {
		select {
		case <-c.ctx.Done():
			return

		case <-c.epochTimer.C:
			c.epoch += 1

			// 1. heatbeat
			if !c.notIdleSendForLastEpoch {
				heatbeatMsg := NewAck(0, 0)
				if err := c.pushIntoCh(c.writeCh, heatbeatMsg); err != nil {
					return
				}
			}
			c.notIdleSendForLastEpoch = false

			// 2. backoff
			c.backoff()

			// 3. lost
			if c.epoch-c.lastRecvEpoch > c.params.EpochLimit {
				// todo: we need to exit?
			}

		case msg := <-c.ackCh:
			// 1. virtual ack
			if msg == nil {
				break
			}

			// 2. todo: connect ack

			// 3. data ack, update window
			v, ok := c.windowMemo[msg.SeqNum]

			// already acked
			if !ok || v == nil {
				continue
			}

			// mark acked
			c.windowMemo[msg.SeqNum] = nil

			i := c.minAckSn + 1
			for {
				v, ok := c.windowMemo[i]
				if !ok {
					err := fmt.Errorf("we didnt send data with sn:%v, but not we recv a ack with it ???", i)
					log.WithError(err).Error("")
					c.cancel()
					return
				}
				if v != nil {
					break
				}
				i += 1
			}
			c.minAckSn = i - 1

			// 4. heatbeat
			{
			}

			break

		case msg := <-c.dataCh:
			// process data

			// send ack
			ackMsg := NewAck(msg.ConnID, msg.SeqNum)
			if err := c.pushIntoCh(c.writeCh, ackMsg); err != nil {
				return
			}

		}

		select {
		case <-c.ctx.Done():
			return
		case msg := <-c.sendCh:
			// assign checksum
			check := CalculateChecksum(c.connID, c.nextSendSn, len(msg.Payload), msg.Payload)
			msg.Checksum = check
			c.pushIntoCh(c.writeCh, msg)

		}

	}
}

func (c *CommonClient) backoff() error {
	min := c.minAckSn + 1
	for i := 0; i < c.params.WindowSize; i++ {
		index := i + min
		if v, ok := c.windowMemo[index]; ok {
			if v == nil {
				continue
			}
			if v.timeout {
				// todo: we need exit? or just skip it
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
		}
	}

	return nil
}

func (c *CommonClient) pushIntoCh(ch chan *Message, msg *Message) error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	case ch <- msg:
		return nil
	}
}

func (c *CommonClient) recvLoop() {
	defer log.Info("exit writeLoop")

	readBytes := make([]byte, MAXN)
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			msg, err := c.recvMessage(readBytes)
			if err != nil {
				log.WithError(err).Error("recvMessage failed")
				continue
			}
			if msg == nil {
				continue
			}

			c.lastRecvEpoch = c.epoch

			if msg.Type == MsgAck || msg.Type == MsgCAck {
				c.pushIntoCh(c.ackCh, msg)
				continue
			}

			if msg.Type == MsgData {
				c.pushIntoCh(c.dataCh, msg)
				continue
			}
		}
	}
}

func (c *CommonClient) recvMessage(readBytes []byte) (*Message, error) {
	c.conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(c.params.EpochMillis)))
	readSize, rAddr, err := c.conn.ReadFromUDP(readBytes)
	if err != nil {
		return nil, err
	}
	if !rAddr.IsSame(c.remoteAddr) {
		return nil, nil
	}
	var msg Message
	if err = json.Unmarshal(readBytes[:readSize], &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (c *CommonClient) writeLoop() {
	for {
		select {
		case <-c.ctx.Done():
			log.Info("exit writeLoop")
			return
		case msg := <-c.writeCh:
			c.notIdleSendForLastEpoch = true

			b, err := json.Marshal(msg)
			if err != nil {
				log.WithError(err).Error("marshal msg failed")
				continue
			}
			_, err = c.write(b)
			if err != nil {
				log.WithError(err).Error("write udp failed")
				continue
			}
		}
	}
}

func (c *CommonClient) write(b []byte) (int, error) {
	if c.connectedConn {
		return c.conn.Write(b)
	} else {
		return c.conn.WriteToUDP(b, c.remoteAddr)
	}
}
