package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
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

type session struct {
	conn *websocket.Conn
	tail *watcher.Tail
	mu   sync.Mutex
}

func (s *session) send(v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn.WriteMessage(websocket.TextMessage, data)
}

type Server struct {
	port     int
	sessions map[*websocket.Conn]*session
	mu       sync.Mutex
}

func New(port int) *Server {
	return &Server{
		port:     port,
		sessions: make(map[*websocket.Conn]*session),
	}
}

func (s *Server) Start() error {
	http.HandleFunc("/ws", s.handleWS)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	go s.heartbeatLoop()

	addr := fmt.Sprintf(":%d", s.port)
	printAccessInfo(s.port)

	return http.ListenAndServe(addr, nil)
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade error: %v", err)
		return
	}

	sess := &session{conn: conn}

	s.mu.Lock()
	s.sessions[conn] = sess
	count := len(s.sessions)
	s.mu.Unlock()

	log.Printf("Cliente conectado (%d ativo(s))", count)

	sess.send(protocol.OutgoingStatus{
		Type:     "status",
		Watching: false,
		File:     "",
		Position: 0,
	})

	defer func() {
		if sess.tail != nil {
			sess.tail.Stop()
		}
		s.mu.Lock()
		delete(s.sessions, conn)
		remaining := len(s.sessions)
		s.mu.Unlock()
		conn.Close()
		log.Printf("Cliente desconectado (%d restante(s))", remaining)
	}()

	for {
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			break
		}
		s.handleMessage(sess, msgBytes)
	}
}

func (s *Server) handleMessage(sess *session, raw []byte) {
	var msg protocol.IncomingMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		log.Printf("Mensagem inválida: %v", err)
		return
	}

	switch msg.Type {
	case "start":
		if sess.tail != nil {
			sess.tail.Stop()
		}

		file := msg.File
		if file == "" {
			log.Printf("Start sem arquivo")
			return
		}

		sess.tail = watcher.New(file, func(line string) {
			sess.send(protocol.OutgoingLine{Type: "line", Data: line})
		})

		backlog := msg.Backlog
		if backlog <= 0 {
			backlog = 500
		}

		lines, err := sess.tail.ReadBacklog(backlog)
		if err != nil {
			log.Printf("Erro ao ler backlog: %v", err)
		} else if len(lines) > 0 {
			sess.send(protocol.OutgoingBacklog{
				Type:  "backlog",
				Lines: lines,
				File:  file,
			})
		}

		sess.tail.Start()
		log.Printf("Monitorando: %s", file)

		sess.send(protocol.OutgoingStatus{
			Type:     "status",
			Watching: true,
			File:     file,
			Position: sess.tail.Position(),
		})

	case "stop":
		if sess.tail != nil {
			sess.tail.Stop()
			log.Printf("Parou de monitorar: %s", sess.tail.FilePath())
		}
		sess.send(protocol.OutgoingStatus{
			Type:     "status",
			Watching: false,
			File:     "",
			Position: 0,
		})

	case "ping":
		sess.send(protocol.OutgoingHeartbeat{Type: "heartbeat"})
	}
}

func (s *Server) heartbeatLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		for _, sess := range s.sessions {
			sess.send(protocol.OutgoingHeartbeat{Type: "heartbeat"})
		}
		s.mu.Unlock()
	}
}

func printAccessInfo(port int) {
	log.Println("═══════════════════════════════════════════════════")
	log.Println("  sdk-agent v0.2.0 iniciado")
	log.Println("═══════════════════════════════════════════════════")

	ips := getLocalIPs()
	for _, ip := range ips {
		log.Printf("  Endereço: %s:%d", ip, port)
	}
	if len(ips) == 0 {
		log.Printf("  Endereço: localhost:%d", port)
	}

	log.Println("")
	log.Println("  Informe o endereço acima no Interpretador SDK")
	log.Println("  para conectar no modo ao vivo.")
	log.Println("═══════════════════════════════════════════════════")
}

func getLocalIPs() []string {
	var ips []string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ips
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			ips = append(ips, ipnet.IP.String())
		}
	}
	return ips
}
