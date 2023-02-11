// Contains the implementation of a LSP server.

package lsp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cmu440/lspnet"
	log "github.com/sirupsen/logrus"
)

type MessageWithAddr struct {
	Message *Message
	Addr    *lspnet.UDPAddr
}

type server struct {
	params *Params
	port   int

	ctx    context.Context
	cancel context.CancelFunc
	g      *sync.WaitGroup

	// epoch
	epochTimer *time.Timer

	udpServer *lspnet.UDPConn

	// key: connId
	clients map[int]*CommonClient

	// key: addr string
	clientMemo map[string]int

	curConnId int

	// recv msg from all clients
	recvMsgCh chan *MessageWithAddr
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", lspnet.JoinHostPort(":", fmt.Sprintf("%v", port)))
	if err != nil {
		log.WithError(err).Error("ResolveUDPAddr failed")
		return nil, err
	}
	udpServer, err := lspnet.ListenUDP("udp", addr)
	if err != nil {
		log.WithError(err).Error("ListenPacket failed")
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.TODO())

	s := &server{
		ctx:        ctx,
		cancel:     cancel,
		port:       port,
		params:     params,
		epochTimer: time.NewTimer(time.Millisecond * time.Duration(params.EpochMillis)),
		g:          &sync.WaitGroup{},
		udpServer:  udpServer,
		recvMsgCh:  make(chan *MessageWithAddr, 100),
	}

	s.g.Add(2)
	go s.mainLoop()
	go s.recvMsgLoop()

	return s, nil
}

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return -1, nil, errors.New("not yet implemented")
}

func (s *server) Write(connId int, payload []byte) error {
	select {
	case <-s.ctx.Done():
		return errClientClosed
	default:
	}

	cli, ok := s.clients[connId]
	if !ok {
		return fmt.Errorf("connid:%v not found", connId)
	}
	return cli.Send(payload)
}

func (s *server) CloseConn(connId int) error {
	select {
	case <-s.ctx.Done():
		return errClientClosed
	default:
	}

	cli, ok := s.clients[connId]
	if !ok {
		return fmt.Errorf("connId not found")
	}

	return cli.Close()

}

func (s *server) Close() error {
	select {
	case <-s.ctx.Done():
		return errClientClosed
	default:
	}

	s.cancel()
	s.g.Wait()

	// todo: close all conn

	// stop server
	return s.udpServer.Close()
}

func (s *server) mainLoop() error {
	defer s.g.Done()
	for {
		select {
		case <-s.ctx.Done():
			return errClientClosed
		case <-s.epochTimer.C:
			// check every client, stop and delete it if necessary
			for k, v := range s.clients {
				if v.Closed() {
					delete(s.clientMemo, v.AddrStr())
					delete(s.clients, k)
				}
			}
			return nil
		case amsg := <-s.recvMsgCh:
			// connect
			if amsg.Message.Type == MsgConnect {
				addr := amsg.Addr.String()
				_, ok := s.clientMemo[addr]
				if ok {
					// client already exist
					continue
				}

				cli := NewCommonClient(s.ctx, amsg.Message.ConnID, s.params, amsg.Message.SeqNum+1, amsg.Addr)
				s.clients[amsg.Message.ConnID] = cli
				s.clientMemo[addr] = 1
				continue
			}

			// ack/data
			// just transfer msg to the client
			cli, ok := s.clients[amsg.Message.ConnID]
			if !ok {
				log.WithField("connId", amsg.Message.ConnID).Error("client not found")
				continue
			}
			cli.Recv(amsg.Message)
		}
	}
	return nil
}

func (c *server) pushIntoCh(ch chan *Message, msg *Message) error {
	select {
	case <-c.ctx.Done():
		return errClientClosed
	case ch <- msg:
		return nil
	}
}

func (s *server) assignConnId() int {
	s.curConnId += 1
	return s.curConnId
}

func (s *server) recvMsgLoop() {
	defer s.g.Done()
	defer log.Info("exit recvMsgLoop ...")

	readBytes := make([]byte, MAXN)
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		msg, addr, err := s.recvMessage(readBytes)
		if err != nil {
			continue
		}

		amsg := &MessageWithAddr{
			Message: msg,
			Addr:    addr,
		}

		select {
		case <-s.ctx.Done():
			return
		case s.recvMsgCh <- amsg:
		}

	}
}

func (c *server) recvMessage(readBytes []byte) (*Message, *lspnet.UDPAddr, error) {
	c.udpServer.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(c.params.EpochMillis)))
	readSize, rAddr, err := c.udpServer.ReadFromUDP(readBytes)
	if err != nil {
		return nil, nil, err
	}
	var msg Message
	if err = json.Unmarshal(readBytes[:readSize], &msg); err != nil {
		return nil, nil, err
	}
	return &msg, rAddr, nil
}
