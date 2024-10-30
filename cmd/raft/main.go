package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/pelageech/go-raft/internal/raft"
)

func main() {
	r, err := raft.New(5)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, 1)

	go func() {
		defer func() { done <- struct{}{} }()
		_ = r.Run(ctx)
	}()

	h := raft.NewHandler(r)
	mux := http.NewServeMux()
	mux.HandleFunc("/nodes", h.Nodes)
	mux.HandleFunc("/journal", h.Journal)
	mux.HandleFunc("/request", h.Request)
	mux.HandleFunc("/kill", h.Kill)
	mux.HandleFunc("/recover", h.Recover)
	mux.HandleFunc("/dump", h.DumpMap)
	mux.HandleFunc("/get", h.Get)

	s := http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		err = s.ListenAndServe()
		if err != nil {
			cancel()
		}
	}()
	<-ch
	log.Print("Shutting down...")
	_ = s.Shutdown(ctx)
	cancel()
	<-ctx.Done()
}
