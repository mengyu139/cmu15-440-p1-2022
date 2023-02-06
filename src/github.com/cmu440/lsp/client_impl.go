// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cmu440/lspnet"
	log "github.com/sirupsen/logrus"
)

const MAXN = 1024

type client struct {
	params *Params

	conn       *lspnet.UDPConn
	remoteAddr *lspnet.UDPAddr
	isn        int
	connID     int

	freeCnt int

	// epoch
	epochTimer *time.Timer
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
		return nil, err
	}
	conn, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}

	c := &client{
		params:     params,
		conn:       conn,
		remoteAddr: addr,
		isn:        initialSeqNum,
		epochTimer: time.NewTimer(0),
	}

	readBytes := make([]byte, MAXN)

	for {
		select {
		case <-c.epochTimer.C:
			c.freeCnt += 1
			if c.freeCnt >= params.EpochLimit {
				err = fmt.Errorf("wait too long before connect")
				log.WithError(err).Error("connect failed")
				return nil, err
			}
		default:
			msg, raddr, err := c.recvMessage(readBytes)
			if err != nil {
				log.WithError(err).Error("recvMessage failed")
				continue
			}
			c.freeCnt = 0

			if !raddr.IsSame(c.remoteAddr) {
				continue
			}

			if msg.SeqNum != 0 {
				continue
			}
			// assgined conn id
			c.connID = msg.ConnID

			return c, nil

		}
	}
}

func (c *client) recvMessage(readBytes []byte) (*Message, *lspnet.UDPAddr, error) {
	c.conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(c.params.EpochMillis)))
	readSize, rAddr, err := c.conn.ReadFromUDP(readBytes)
	if err != nil {
		return nil, nil, err
	}
	var msg Message
	if err = json.Unmarshal(readBytes[:readSize], &msg); err != nil {
		return nil, nil, err
	}
	return &msg, rAddr, nil
}

// send connect intention and wait for ack response
func (c *client) Ack(params *Params) error {
	_, err := c.conn.WriteToUDP([]byte("ack"), c.remoteAddr)
	if err != nil {
		return err
	}

	rcv := make([]byte, 1024)
	n, err := c.conn.Read(rcv)
	if err != nil {
		return err
	}

	rcvs := string(rcv[0:n])
	rcvs = rcvs

	return nil
}

func (c *client) ConnID() int {
	return -1
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return nil, errors.New("not yet implemented")
}

func (c *client) Write(payload []byte) error {
	return errors.New("not yet implemented")
}

func (c *client) Close() error {
	return errors.New("not yet implemented")
}
