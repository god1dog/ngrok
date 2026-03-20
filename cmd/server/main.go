package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"ngrok-lite/internal/proto"
)

type queuedRequest struct {
	Message proto.TunnelMessage
}

type tunnel struct {
	id      string
	reqCh   chan queuedRequest
	pending map[string]chan proto.TunnelMessage
	mu      sync.Mutex
}

type serverState struct {
	mu      sync.RWMutex
	tunnels map[string]*tunnel
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	publicBase := flag.String("public-base", "http://localhost:8080", "public base URL")
	flag.Parse()

	state := &serverState{tunnels: map[string]*tunnel{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/register", state.registerHandler(*publicBase))
	mux.HandleFunc("/next", state.nextRequest)
	mux.HandleFunc("/respond", state.receiveResponse)
	mux.HandleFunc("/t/", state.tunnelProxy)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ngrok-lite server up")
	})

	log.Printf("server listening on %s", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}

func (s *serverState) registerHandler(publicBase string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req proto.RegisterRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		id := req.ID
		if id == "" {
			id = randID(8)
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.tunnels[id]; ok {
			http.Error(w, "id already exists", http.StatusConflict)
			return
		}
		s.tunnels[id] = &tunnel{id: id, reqCh: make(chan queuedRequest, 128), pending: map[string]chan proto.TunnelMessage{}}

		resp := proto.RegisterResponse{ClientID: id, TunnelURL: fmt.Sprintf("%s/t/%s", publicBase, id)}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func (s *serverState) nextRequest(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}
	tun := s.getTunnel(id)
	if tun == nil {
		http.Error(w, "unknown id", http.StatusNotFound)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	select {
	case req := <-tun.reqCh:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(req.Message)
	case <-ctx.Done():
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *serverState) receiveResponse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := r.URL.Query().Get("id")
	tun := s.getTunnel(id)
	if tun == nil {
		http.Error(w, "unknown id", http.StatusNotFound)
		return
	}
	var msg proto.TunnelMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	tun.mu.Lock()
	ch := tun.pending[msg.RequestID]
	if ch != nil {
		delete(tun.pending, msg.RequestID)
	}
	tun.mu.Unlock()
	if ch != nil {
		ch <- msg
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *serverState) tunnelProxy(w http.ResponseWriter, r *http.Request) {
	rest := r.URL.Path[len("/t/"):]
	id, reqPath := split(rest)
	tun := s.getTunnel(id)
	if tun == nil {
		http.NotFound(w, r)
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	requestID := randID(10)
	msg := proto.TunnelMessage{
		Type:      "request",
		RequestID: requestID,
		Method:    r.Method,
		Path:      "/" + reqPath,
		Query:     r.URL.RawQuery,
		Headers:   proto.HeaderFromHTTP(r.Header),
		BodyB64:   proto.EncodeBase64(body),
	}

	respCh := make(chan proto.TunnelMessage, 1)
	tun.mu.Lock()
	tun.pending[requestID] = respCh
	tun.mu.Unlock()

	select {
	case tun.reqCh <- queuedRequest{Message: msg}:
	case <-time.After(2 * time.Second):
		http.Error(w, "tunnel queue full", http.StatusBadGateway)
		return
	}

	select {
	case resp := <-respCh:
		if resp.Error != "" {
			http.Error(w, resp.Error, http.StatusBadGateway)
			return
		}
		for k, vals := range resp.Headers.ToHTTP() {
			for _, v := range vals {
				w.Header().Add(k, v)
			}
		}
		status := resp.StatusCode
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		decoded, _ := proto.DecodeBase64(resp.BodyB64)
		_, _ = w.Write(decoded)
	case <-time.After(30 * time.Second):
		http.Error(w, "upstream timeout", http.StatusGatewayTimeout)
	}
}

func (s *serverState) getTunnel(id string) *tunnel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tunnels[id]
}

func randID(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = letters[int(b[i])%len(letters)]
	}
	return string(b)
}

func split(path string) (id, rest string) {
	for i := 0; i < len(path); i++ {
		if path[i] == '/' {
			return path[:i], path[i+1:]
		}
	}
	return path, ""
}
