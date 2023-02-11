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

type client struct {
	// control
	ctx    context.Context
	cancel context.CancelFunc
	g      *sync.WaitGroup

	params *Params
	conn   *lspnet.UDPConn
	connID int

	commonClient *CommonClient
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

	connect := func() (int, error) {
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

			if msg.Type == MsgAck && msg.ConnID != 0 {
				connID = msg.ConnID
				done = true
				break
			}
		}
		if !done {
			err := fmt.Errorf("connect failed")
			log.WithError(err).Error("")
			return 0, err
		}
		return connID, nil
	}

	connID, err := connect()
	if err != nil {
		conn.Close()
		return nil, err
	}

	// build common client
	ctx, cancel := context.WithCancel(context.Background())
	commonClient := NewCommonClient(ctx, connID, params, initialSeqNum+1, nil)
	c := &client{
		ctx:          ctx,
		cancel:       cancel,
		connID:       connID,
		conn:         conn,
		commonClient: commonClient,
	}

	return c, nil
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

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, errClientClosed
	default:
	}
	return c.commonClient.Read()
}

func (c *client) Write(payload []byte) error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	default:
	}
	return c.commonClient.Send(payload)
}

func (c *client) Close() error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	default:
	}
	c.commonClient.Close()
	c.cancel()
	c.conn.Close()
	return nil
}
