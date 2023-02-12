// Contains the implementation of a LSP server.

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

type MessageWithAddr struct {
	message *Message
	addr    *lspnet.UDPAddr
}

func NewMessageWithAddr(msg *Message, addr *lspnet.UDPAddr) *MessageWithAddr {
	return &MessageWithAddr{
		message: msg,
		addr:    addr,
	}
}

type server struct {
	params *Params
	port   int

	ctx    context.Context
	cancel context.CancelFunc
	g      *sync.WaitGroup

	// epoch
	epochTicker *time.Ticker

	udpServer *lspnet.UDPConn

	curConnId int

	// recv data from udp
	recvCh      chan *MessageWithAddr
	closeConnCh chan int

	// read data from all schedluers
	readCh chan *Message

	dataForReadCh chan *MessageWithErr

	// data in this channel will be wtiten to udp directly
	dataForWriteUPDCh chan *MessageWithAddr

	// key: connId
	schedulersMtx sync.Mutex

	schedulers map[int]*Scheduler
	// key: addr string
	schedulersWithAddr map[string]*Scheduler

	runningCnt int
	closing    bool
	closed     chan int
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", lspnet.JoinHostPort("", fmt.Sprintf("%v", port)))
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
		ctx:                ctx,
		cancel:             cancel,
		port:               port,
		params:             params,
		epochTicker:        time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis)),
		g:                  &sync.WaitGroup{},
		udpServer:          udpServer,
		recvCh:             make(chan *MessageWithAddr, 100),
		closeConnCh:        make(chan int, 10),
		schedulers:         make(map[int]*Scheduler),
		schedulersWithAddr: make(map[string]*Scheduler),
		readCh:             make(chan *Message, 100),
		dataForReadCh:      make(chan *MessageWithErr, 1000),
		dataForWriteUPDCh:  make(chan *MessageWithAddr, 1000),
		closed:             make(chan int),
	}

	s.g.Add(3)
	go s.mainLoop()
	go s.recvLoop()
	go s.writeLoop()

	return s, nil
}

func (s *server) Read() (int, []byte, error) {
	select {
	case <-s.ctx.Done():
		return 0, nil, errClientClosed

	case msg := <-s.dataForReadCh:
		return msg.message.ConnID, msg.message.Payload, msg.err
	}
}

func (s *server) Write(connId int, payload []byte) error {
	select {
	case <-s.ctx.Done():
		return errClientClosed

	default:
	}

	s.schedulersMtx.Lock()
	v, ok := s.schedulers[connId]
	s.schedulersMtx.Unlock()

	if !ok {
		return errClientClosed
	}
	if v.Lost(false) {
		return errClientLost
	}

	if v.Closing() {
		return errClientClosing
	}
	if v.Closed() {
		return errClientClosed
	}

	v.Send(payload)

	return nil
}

func (s *server) CloseConn(connId int) error {

	select {
	case <-s.ctx.Done():
		return errClientClosed
	default:
	}

	s.schedulersMtx.Lock()
	_, ok := s.schedulers[connId]
	s.schedulersMtx.Unlock()

	if !ok {
		return errClientClosed
	}

	return nil

}

func (s *server) Close() error {
	select {
	case <-s.ctx.Done():
		return errClientClosed
	default:
	}

	// close all conn
	s.schedulersMtx.Lock()
	for _, v := range s.schedulers {
		state := v.State()
		if state == StateRuning {
			log.WithField("connID", v.ConnID()).Info("is closing")
			v.Close()
		}
	}
	s.schedulersMtx.Unlock()

	// wait for closed signal
	select {
	case <-s.closed:
	}

	// stop server
	return s.udpServer.Close()
}

func (s *server) mainLoop() {
	defer s.g.Done()
	defer log.Info("exit mainLoop")

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.epochTicker.C:
			s.schedulersMtx.Lock()
			for _, v := range s.schedulers {
				state := v.Tick()

				if state == StateClosed {
					log.WithField("connID", v.ConnID()).Info("closed")

					v.Cancel()
					delete(s.schedulers, v.ConnID())
					delete(s.schedulersWithAddr, v.AddrStr())

					s.runningCnt -= 1
					continue
				}

				// check lost
				v.Lost(true)

			}
			s.schedulersMtx.Unlock()

			// close
			select {
			case <-s.closed:
				log.Info("server is closed")
			default:
				if s.closing && s.runningCnt == 0 {
					log.Info("all running scheduler are closed, server is ready to be close")
				}
				close(s.closed)
			}

		case amsg := <-s.recvCh:
			if amsg.message.Type == MsgConnect {
				addr := amsg.addr.String()
				connId := 0
				v, ok := s.schedulersWithAddr[addr]
				if !ok {
					// memo
					connId = s.assignConnId()
					sch := NewScheduler(s.ctx, connId, amsg.message.SeqNum, s.params, amsg.addr)

					s.schedulersMtx.Lock()
					s.schedulers[connId] = sch
					s.schedulersWithAddr[addr] = sch
					s.schedulersMtx.Unlock()

					// monitor
					go s.readMonitor(sch.Done(), sch.RecvOutput())
					go s.sendMonitor(sch.Done(), amsg.addr, sch.SendOutput())

					s.runningCnt += 1
				} else {
					connId = v.ConnID()
				}

				//send ack back
				s.ackBack(amsg.addr, connId, amsg.message.SeqNum)

			} else {
				// ack or data
				s.schedulersMtx.Lock()
				addr := amsg.addr.String()
				v, ok := s.schedulersWithAddr[addr]
				s.schedulersMtx.Unlock()

				if !ok {
					continue
				}

				if v.Lost(false) {
					continue
				}

				if amsg.message.Type == MsgData {
					v.Recv(amsg.message)
				} else if amsg.message.Type == MsgAck || amsg.message.Type == MsgCAck {
					v.Ack(amsg.message)
				}
			}
		case c := <-s.closeConnCh:
			s.schedulersMtx.Lock()
			sch, ok := s.schedulers[c]
			if ok {
				sch.Close()
			}
			s.schedulersMtx.Unlock()
		}
	}
}

func (s *server) ackBack(addr *lspnet.UDPAddr, connId int, sn int) error {
	msg := NewAck(connId, sn)
	amsg := NewMessageWithAddr(msg, addr)

	select {
	case <-s.ctx.Done():
		return errClientClosed
	case s.dataForWriteUPDCh <- amsg:
	}
	return nil
}

func (s *server) recvLoop() {
	defer s.g.Done()
	defer log.Info("exit readLoop")

	readBytes := make([]byte, MAXN)

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		msg, addr, err := s.recvMessage(readBytes)
		if err != nil {
			log.WithError(err).Debug("recvMessage failed")
			continue
		}

		select {
		case <-s.ctx.Done():
			return
		case s.recvCh <- NewMessageWithAddr(msg, addr):
		}

	}
}

// write data to udp
func (s *server) writeLoop() {
	defer s.g.Done()
	defer log.Info("exit writeLoop")

	for {
		select {
		case <-s.ctx.Done():
			return
		case amsg := <-s.dataForWriteUPDCh:
			b, err := json.Marshal(amsg.message)
			if err != nil {
				log.WithError(err).Error("marshal failed")
				continue
			}
			_, err = s.udpServer.WriteToUDP(b, amsg.addr)
			if err != nil {
				log.WithError(err).Error("server WriteToUDP failed")
				continue
			}
		}
	}
}

func (s *server) readMonitor(done <-chan struct{}, dataCh <-chan *MessageWithErr) {
	for {
		select {
		case <-done:
			return
		case msg := <-dataCh:
			select {
			case <-s.ctx.Done():
				return
			case s.dataForReadCh <- msg:

			}
		}
	}
}

func (s *server) sendMonitor(done <-chan struct{}, addr *lspnet.UDPAddr, dataCh <-chan *Message) {
	for {
		select {
		case <-done:
			return
		case msg := <-dataCh:
			select {
			case <-s.ctx.Done():
				return
			case s.dataForWriteUPDCh <- NewMessageWithAddr(msg, addr):
			}
		}
	}
}

func (s *server) assignConnId() int {
	s.curConnId += 1
	return s.curConnId
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
