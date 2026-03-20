package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"ngrok-lite/internal/proto"
)

type requestLog struct {
	Time       time.Time `json:"time"`
	Method     string    `json:"method"`
	Path       string    `json:"path"`
	StatusCode int       `json:"status_code"`
	DurationMS int64     `json:"duration_ms"`
}

type dashboard struct {
	mu      sync.Mutex
	entries []requestLog
}

func (d *dashboard) add(e requestLog) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries = append([]requestLog{e}, d.entries...)
	if len(d.entries) > 100 {
		d.entries = d.entries[:100]
	}
}

func (d *dashboard) list() []requestLog {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]requestLog, len(d.entries))
	copy(out, d.entries)
	return out
}

func main() {
	server := flag.String("server", "http://localhost:8080", "server base URL")
	local := flag.String("local", "http://localhost:3000", "local upstream URL")
	id := flag.String("id", "", "client id (optional)")
	dashAddr := flag.String("dashboard", ":4040", "dashboard listen address")
	flag.Parse()

	clientID, tunnelURL, err := register(*server, *id)
	if err != nil {
		log.Fatalf("register failed: %v", err)
	}
	log.Printf("tunnel ready: %s", tunnelURL)

	db := &dashboard{}
	go serveDashboard(*dashAddr, tunnelURL, *local, db)

	for {
		if err := pollNext(*server, clientID, *local, db); err != nil {
			log.Printf("poll error: %v", err)
			time.Sleep(1 * time.Second)
		}
	}
}

func register(server, id string) (clientID, tunnelURL string, err error) {
	payload := proto.RegisterRequest{ID: id}
	b, _ := json.Marshal(payload)
	resp, err := http.Post(strings.TrimRight(server, "/")+"/register", "application/json", bytes.NewReader(b))
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", "", fmt.Errorf("register status %d: %s", resp.StatusCode, string(body))
	}
	var rr proto.RegisterResponse
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return "", "", err
	}
	return rr.ClientID, rr.TunnelURL, nil
}

func pollNext(server, id, local string, db *dashboard) error {
	hc := &http.Client{Timeout: 35 * time.Second}
	resp, err := hc.Get(strings.TrimRight(server, "/") + "/next?id=" + id)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("next status %d: %s", resp.StatusCode, string(b))
	}

	var msg proto.TunnelMessage
	if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
		return err
	}
	res := handleRequest(msg, local, db)
	return sendResponse(server, id, res)
}

func sendResponse(server, id string, msg proto.TunnelMessage) error {
	b, _ := json.Marshal(msg)
	req, _ := http.NewRequest(http.MethodPost, strings.TrimRight(server, "/")+"/respond?id="+id, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("respond status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func handleRequest(msg proto.TunnelMessage, local string, db *dashboard) proto.TunnelMessage {
	start := time.Now()
	respMsg := proto.TunnelMessage{Type: "response", RequestID: msg.RequestID}
	defer func() {
		db.add(requestLog{Time: start, Method: msg.Method, Path: msg.Path, StatusCode: respMsg.StatusCode, DurationMS: time.Since(start).Milliseconds()})
	}()

	decoded, err := proto.DecodeBase64(msg.BodyB64)
	if err != nil {
		respMsg.Error = "invalid body"
		respMsg.StatusCode = 502
		return respMsg
	}

	target := strings.TrimRight(local, "/") + msg.Path
	if msg.Query != "" {
		target += "?" + msg.Query
	}
	req, err := http.NewRequest(msg.Method, target, bytes.NewReader(decoded))
	if err != nil {
		respMsg.Error = err.Error()
		respMsg.StatusCode = 502
		return respMsg
	}
	req.Header = msg.Headers.ToHTTP()

	hc := &http.Client{Timeout: 25 * time.Second}
	resp, err := hc.Do(req)
	if err != nil {
		respMsg.Error = err.Error()
		respMsg.StatusCode = 502
		return respMsg
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	respMsg.StatusCode = resp.StatusCode
	respMsg.Status = resp.Status
	respMsg.Headers = proto.HeaderFromHTTP(resp.Header)
	respMsg.BodyB64 = proto.EncodeBase64(respBody)
	return respMsg
}

func serveDashboard(addr, tunnelURL, local string, db *dashboard) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/requests", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(db.list())
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(dashboardHTML(tunnelURL, local)))
	})
	log.Printf("dashboard on http://localhost%s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("dashboard stopped: %v", err)
	}
}

func dashboardHTML(tunnelURL, local string) string {
	return fmt.Sprintf(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"/>
<title>ngrok-lite dashboard</title>
<style>body{font-family:sans-serif;max-width:1000px;margin:30px auto;padding:0 12px}table{width:100%%;border-collapse:collapse}th,td{border-bottom:1px solid #ddd;padding:8px}code{background:#f4f4f4;padding:2px 6px;border-radius:6px}</style>
</head><body>
<h2>ngrok-lite dashboard</h2>
<p>Public URL: <code>%s</code></p>
<p>Forwarding to: <code>%s</code></p>
<table><thead><tr><th>Time</th><th>Method</th><th>Path</th><th>Status</th><th>ms</th></tr></thead><tbody id="rows"></tbody></table>
<script>
async function tick(){
 const r=await fetch('/api/requests');
 const items=await r.json();
 const rows=document.getElementById('rows');
 rows.innerHTML = items.map(function(i){return '<tr><td>'+new Date(i.time).toLocaleTimeString()+'</td><td>'+i.method+'</td><td>'+i.path+'</td><td>'+i.status_code+'</td><td>'+i.duration_ms+'</td></tr>';}).join('');
}
setInterval(tick,1000); tick();
</script>
</body></html>`, tunnelURL, local)
}
