package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/GersonOliveira1474/sdk-agent/protocol"
	"github.com/GersonOliveira1474/sdk-agent/watcher"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Server struct {
	port    int
	tail    *watcher.Tail
	clients map[*websocket.Conn]bool
	mu      sync.Mutex
}

func New(port int, defaultFile string) *Server {
	s := &Server{
		port:    port,
		clients: make(map[*websocket.Conn]bool),
	}

	s.tail = watcher.New(defaultFile, func(line string) {
		s.broadcast(protocol.OutgoingLine{Type: "line", Data: line})
	})

	return s
}

func (s *Server) Start() error {
	http.HandleFunc("/ws", s.handleWS)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	go s.heartbeatLoop()

	addr := fmt.Sprintf(":%d", s.port)
	log.Printf("sdk-agent listening on ws://localhost%s/ws", addr)

	if s.tail.FilePath() != "" {
		log.Printf("Auto-watching: %s", s.tail.FilePath())
		s.tail.Start()
	}

	return http.ListenAndServe(addr, nil)
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade error: %v", err)
		return
	}

	s.mu.Lock()
	s.clients[conn] = true
	s.mu.Unlock()

	log.Printf("Client connected (%d total)", len(s.clients))

	s.sendStatus(conn)

	defer func() {
		s.mu.Lock()
		delete(s.clients, conn)
		s.mu.Unlock()
		conn.Close()
		log.Printf("Client disconnected (%d remaining)", len(s.clients))
	}()

	for {
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			break
		}
		s.handleMessage(conn, msgBytes)
	}
}

func (s *Server) handleMessage(conn *websocket.Conn, raw []byte) {
	var msg protocol.IncomingMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		log.Printf("Invalid message: %v", err)
		return
	}

	switch msg.Type {
	case "start":
		if msg.File != "" {
			s.tail.Stop()
			s.tail.SetFile(msg.File)
		}
		backlog := msg.Backlog
		if backlog <= 0 {
			backlog = 500
		}
		lines, err := s.tail.ReadBacklog(backlog)
		if err != nil {
			log.Printf("Backlog error: %v", err)
		} else if len(lines) > 0 {
			s.sendTo(conn, protocol.OutgoingBacklog{
				Type:  "backlog",
				Lines: lines,
				File:  s.tail.FilePath(),
			})
		}
		s.tail.Start()
		log.Printf("Started watching: %s", s.tail.FilePath())
		s.broadcastStatus()

	case "stop":
		s.tail.Stop()
		log.Printf("Stopped watching")
		s.broadcastStatus()

	case "ping":
		s.sendTo(conn, protocol.OutgoingHeartbeat{Type: "heartbeat"})
	}
}

func (s *Server) sendStatus(conn *websocket.Conn) {
	s.sendTo(conn, protocol.OutgoingStatus{
		Type:     "status",
		Watching: s.tail.IsRunning(),
		File:     s.tail.FilePath(),
		Position: s.tail.Position(),
	})
}

func (s *Server) broadcastStatus() {
	s.broadcast(protocol.OutgoingStatus{
		Type:     "status",
		Watching: s.tail.IsRunning(),
		File:     s.tail.FilePath(),
		Position: s.tail.Position(),
	})
}

func (s *Server) sendTo(conn *websocket.Conn, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	conn.WriteMessage(websocket.TextMessage, data)
}

func (s *Server) broadcast(v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for conn := range s.clients {
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			conn.Close()
			delete(s.clients, conn)
		}
	}
}

func (s *Server) heartbeatLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.broadcast(protocol.OutgoingHeartbeat{Type: "heartbeat"})
	}
}
