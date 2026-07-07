package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	addr := ":1883"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	srv := NewServer(addr)
	if err := srv.Start(); err != nil {
		log.Fatalf("server: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down...")
	srv.Stop()
}
