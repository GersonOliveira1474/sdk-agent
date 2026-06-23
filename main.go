package main

import (
	"flag"
	"log"

	"github.com/GersonOliveira1474/sdk-agent/server"
)

func main() {
	port := flag.Int("port", 9800, "WebSocket server port")
	file := flag.String("file", "", "Log file to watch immediately on startup")
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Printf("sdk-agent v0.1.0 starting...")

	srv := server.New(*port, *file)
	if err := srv.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
