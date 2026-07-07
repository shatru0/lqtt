package main

import (
	"log"
	"net"
)

type Server struct {
	addr   string
	tm     *TopicManager
	auth   Authenticator
	ln     net.Listener
	closed chan struct{}
}

func NewServer(addr string) *Server {
	return &Server{
		addr:   addr,
		tm:     NewTopicManager(),
		auth:   AllowAllAuthenticator{},
		closed: make(chan struct{}),
	}
}

func (s *Server) SetAuthenticator(auth Authenticator) {
	s.auth = auth
}

func (s *Server) Start() error {
	var err error
	s.ln, err = net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	log.Printf("MQTT broker listening on %s", s.addr)

	go s.acceptLoop()
	return nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.closed:
				return
			default:
			}
			log.Printf("accept: %v", err)
			continue
		}
		go handleClient(conn, s.tm, s.auth)
	}
}

func (s *Server) Stop() {
	close(s.closed)
	s.ln.Close()
}
