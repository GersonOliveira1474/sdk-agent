package main

import (
	"flag"
	"log"

	"github.com/GersonOliveira1474/sdk-agent/server"
)

func main() {
	port := flag.Int("port", 9800, "Porta do servidor WebSocket")
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime)

	srv := server.New(*port)
	if err := srv.Start(); err != nil {
		log.Fatalf("Erro ao iniciar servidor: %v", err)
	}
}
