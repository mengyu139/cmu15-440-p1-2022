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
	errClientClosing = fmt.Errorf("client is closing")
	errClientClosed  = fmt.Errorf("client is alread closed")
	errClientLost    = fmt.Errorf("conn is lost")
)

type client struct {
	// control
	ctx    context.Context
	cancel context.CancelFunc

	conn   *lspnet.UDPConn
	connID int

	// sendSch   *SendScheduler
	// recvSch   *RecvScheduler
	scheduler *Scheduler
	params    *Params

	epoch         int
	lastSendEpoch int
	lastRecvEpoch int // lastest epoch stamp when recieve msg from server

	readCh      chan *Message
	writeCh     chan *Message
	sendCh      chan []byte
	epochTicker *time.Ticker
	g           *sync.WaitGroup
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

	connect := func(sn int) (int, error) {
		// try connect
		readBytes := make([]byte, MAXN)
		connID := 0
		done := false
		for i := 0; i < params.EpochLimit; i++ {
			connectMsg := NewConnect(initialSeqNum)
			b, err := json.Marshal(connectMsg)
			if err != nil {
				log.WithError(err).Error("marshal connectMsg failed")
				return 0, err
			}
			conn.SetDeadline(time.Now().Add(time.Millisecond * time.Duration(params.EpochMillis)))
			if _, err := conn.Write(b); err != nil {
				log.WithError(err).Error("conn write bytes failed")
				return 0, err
			}

			msg, err := recvMessage(conn, readBytes, params)
			if err != nil {
				log.WithError(err).Error("conn recvMessage failed")
				return 0, err
			}

			log.WithField("msg", msg).Trace("recieved ack from server")
			if msg.Type == MsgAck && msg.ConnID != 0 && msg.SeqNum == sn {
				connID = msg.ConnID
				done = true
				break
			}
		}
		if !done {
			err := fmt.Errorf("connect failed, no recieve msg from server or msg is invalid")
			log.WithError(err).Error("")
			return 0, err
		}
		return connID, nil
	}

	connID, err := connect(initialSeqNum)
	if err != nil {
		conn.Close()
		return nil, err
	}

	log.WithField("connID", connID).Trace("client conn online, working ...")

	// build common client
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		ctx:         ctx,
		cancel:      cancel,
		connID:      connID,
		conn:        conn,
		scheduler:   NewScheduler(ctx, connID, initialSeqNum, params, addr),
		params:      params,
		readCh:      make(chan *Message, 1000),
		writeCh:     make(chan *Message, 100),
		sendCh:      make(chan []byte, 100),
		epochTicker: time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis)),
		g:           &sync.WaitGroup{},
	}

	log.WithField("params", params).Debug("init client")

	c.g.Add(3)
	go c.mainLoop()
	go c.recvLoop()
	go c.writeLoop()

	return c, nil
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, errClientClosed
	case amsg := <-c.scheduler.RecvOutput():
		if amsg.err != nil {
			return nil, amsg.err
		}
		return amsg.message.Payload, nil
	}

}

func (c *client) Write(payload []byte) error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	case c.sendCh <- payload:
		return nil
	}
}

func (c *client) Close() error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	default:
	}

	c.cancel()
	c.conn.Close()
	return nil
}

func (c *client) mainLoop() {
	defer c.g.Done()
	defer log.Info("exit mainLoop")

	for {
		select {
		case <-c.ctx.Done():
			return

		case payload := <-c.sendCh:
			c.scheduler.Send(payload)

		case <-c.epochTicker.C:
			c.scheduler.Tick(c.epoch)
			c.epoch += 1
			log.WithField("epoch", c.epoch).Debug("tick...")

		case msg := <-c.readCh:
			if msg == nil {
				continue
			}
			c.lastRecvEpoch = c.epoch

			if msg.Type == MsgAck || msg.Type == MsgCAck {
				c.scheduler.Ack(msg)
				continue
			}

			if msg.Type == MsgData {
				c.scheduler.Recv(msg)

				// send ack back
				c.ackBack(c.connID, msg.SeqNum)

				continue
			}
		}
	}
}

func (c *client) ackBack(connId int, sn int) error {
	msg := NewAck(connId, sn)

	select {
	case <-c.ctx.Done():
		return errClientClosed
	case c.writeCh <- msg:
		log.WithFields(log.Fields{
			"msg": msg,
		}).Debug("ack back")
		return nil
	}
}

func (c *client) recvLoop() {
	defer c.g.Done()
	defer log.Info("exit recvLoop")

	readBytes := make([]byte, MAXN)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		msg, err := recvMessage(c.conn, readBytes, c.params)
		if err != nil {
			continue
		}

		select {
		case <-c.ctx.Done():
			return
		case c.readCh <- msg:
		}

	}
}

func (c *client) writeLoop() {
	defer c.g.Done()
	defer log.Info("exit writeLoop")
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg := <-c.scheduler.SendOutput():
			c.lastSendEpoch = c.epoch
			writeUDP(c.params, c.conn, msg)

		case msg := <-c.writeCh:
			c.lastSendEpoch = c.epoch
			writeUDP(c.params, c.conn, msg)
		}

	}
}

func writeUDP(params *Params, conn *lspnet.UDPConn, msg *Message) error {
	conn.SetDeadline(time.Now().Add(time.Millisecond * time.Duration(params.EpochMillis)))

	b, err := json.Marshal(msg)
	if err != nil {
		log.WithError(err).Error("marshal msg failed")
		return err
	}

	logger := log.WithField("type", msg.Type).WithField("sn", msg.SeqNum).WithField("->", "UPD")

	_, err = conn.Write(b)
	if err != nil {
		logger.WithError(err).Error("Write UDP failed")

	}
	logger.Debug("writed")
	return err
}

func recvMessage(conn *lspnet.UDPConn, readBytes []byte, params *Params) (*Message, error) {
	conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(params.EpochMillis)))
	readSize, err := conn.Read(readBytes)
	if err != nil {
		return nil, err
	}
	var msg Message
	if err = json.Unmarshal(readBytes[:readSize], &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
