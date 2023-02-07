// Contains the implementation of a LSP client.

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

var (
	errClientClosed = fmt.Errorf("client is alread closed")
)

const MAXN = 1024

type MessageData struct {
	Message    *Message
	curBackoff int // 0 1 2 4 8 ...
	lastTime   time.Time
	firstTime  time.Time
	timeout    bool
}

func CreateMessageData(msg *Message) *MessageData {
	return &MessageData{
		Message: msg,
	}
}

type client struct {
	// control
	ctx    context.Context
	cancel context.CancelFunc
	g      *sync.WaitGroup

	params     *Params
	conn       *lspnet.UDPConn
	remoteAddr *lspnet.UDPAddr
	connID     int

	// var
	initialSeqNum int
	nextSendSn    int
	nextRecvSn    int
	freeCnt       int

	dataPool map[int][]byte

	// epoch
	epochTimer *time.Timer

	// chan
	// message should be push to this chan before writing to udp
	sendMsgCh       chan *Message
	sendDataCh      chan *MessageData
	sendAckCh       chan *Message
	connectLoopDone chan int
	connectDone     chan int
	dataBuffer      chan []byte

	ackSlot   int
	ackWindow int

	// memo
	// key: isn
	windowMemo map[int]*MessageData
	unAckCnt   int
	minAckSn   int
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// initialSeqNum is an int representing the Initial Sequence Number (ISN) this
// client must use. You may assume that sequence numbers do not wrap around.
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, initialSeqNum int, params *Params) (Client, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		log.WithError(err).Error("ResolveUDPAddr failed")
		return nil, err
	}
	conn, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		log.WithError(err).Error("dial failed")
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.TODO())

	c := &client{
		ctx:             ctx,
		cancel:          cancel,
		params:          params,
		conn:            conn,
		remoteAddr:      addr,
		initialSeqNum:   initialSeqNum,
		nextSendSn:      initialSeqNum + 1,
		nextRecvSn:      initialSeqNum + 1,
		epochTimer:      time.NewTimer(0),
		sendMsgCh:       make(chan *Message, 1),
		sendDataCh:      make(chan *MessageData, 1),
		sendAckCh:       make(chan *Message, params.MaxUnackedMessages+10),
		connectDone:     make(chan int, 1),
		connectLoopDone: make(chan int, 1),
		g:               &sync.WaitGroup{},
		dataBuffer:      make(chan []byte, params.WindowSize*2+10),
		dataPool:        make(map[int][]byte, 10),
		ackSlot:         params.MaxUnackedMessages,
		ackWindow:       params.WindowSize,

		// we consider sn:0 is acked
		windowMemo: map[int]*MessageData{
			0: nil,
		},
	}
	for i := 0; i < params.MaxUnackedMessages; i++ {
		c.sendAckCh <- nil
	}

	c.g.Add(3)
	go c.connectLoop()
	go c.recvLoop()
	go c.sendLoop()

	select {
	case <-c.connectDone:
		c.g.Add(3)
		go c.sendDataLoop()
		go c.sendBackoffLoop()
		go c.monitorLoop()
		return c, nil
	case <-c.ctx.Done():
		return nil, errClientClosed
	}
}
func (c *client) monitorLoop() {
	defer c.g.Done()
	select {
	case <-c.ctx.Done():
		c.epochTimer.Stop()
		c.conn.Close()
	}
}

func (c *client) connectLoop() error {
	defer c.g.Done()
	for {
		select {
		case <-c.ctx.Done():
			return errClientClosed

		case <-c.epochTimer.C:
			c.freeCnt += 1

			// client is lost
			if c.freeCnt >= c.params.EpochLimit {
				err := fmt.Errorf("wait too long before connect")
				log.WithError(err).Error("connect failed")
				return err
			}

			// resend connect message
			msg := NewConnect(c.initialSeqNum)

			c.sendMsg(msg)

		case <-c.connectDone:
			select {
			case c.connectDone <- 1:
			default:
			}

			return nil

		}
	}
}

func (c *client) recvLoop() {
	defer c.g.Done()

	readBytes := make([]byte, MAXN)

	for {
		select {
		case <-c.ctx.Done():
			log.WithField("func", "recvLoop").Info("exit")
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

			c.freeCnt = 0

			if msg.Type == MsgData {
				// store data in buffer or pool
				c.storeDara(msg)

				// send ack
				ack := NewAck(msg.ConnID, msg.SeqNum)
				c.sendAck(ack)
				continue
			}

			// server ack for connect
			if msg.Type == MsgAck && msg.SeqNum == 0 {
				select {
				case c.connectLoopDone <- 1:
					c.connID = msg.ConnID
				default:
				}
				continue
			}

			// server ack for data msg
			if msg.Type == MsgAck {
				c.sendAck(msg)
			}
		}
	}
}

func (c *client) storeDara(msg *Message) error {
	if msg.SeqNum == c.nextRecvSn {
		c.nextRecvSn += 1
		c.dataBuffer <- msg.Payload

		// find other data in order
		for {
			if payload, ok := c.dataPool[c.nextRecvSn]; ok {
				c.nextRecvSn += 1
				c.dataBuffer <- payload

				//remove reference
				c.dataPool[c.nextRecvSn] = nil
				continue
			}
			break
		}
	} else {
		c.dataPool[msg.SeqNum] = msg.Payload
	}
	return nil
}

func (c *client) recvMessage(readBytes []byte) (*Message, error) {
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

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, errClientClosed
	case data := <-c.dataBuffer:
		return data, nil
	}
}

// msg -> sendDataCh -> sendMsgCh
func (c *client) Write(payload []byte) error {
	check := CalculateChecksum(c.connID, c.nextSendSn, len(payload), payload)
	msg := NewData(c.connID, c.nextSendSn, len(payload), payload, check)
	c.nextSendSn += 1
	return c.sendData(CreateMessageData(msg))
}

func (c *client) Close() error {
	c.cancel()
	c.g.Wait()

	return nil
}
