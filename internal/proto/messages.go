package proto

import (
	"encoding/base64"
	"net/http"
)

type RegisterRequest struct {
	ID string `json:"id"`
}

type RegisterResponse struct {
	TunnelURL string `json:"tunnel_url"`
	ClientID  string `json:"client_id"`
}

type TunnelMessage struct {
	Type string `json:"type"`

	RequestID string `json:"request_id,omitempty"`
	Method    string `json:"method,omitempty"`
	Path      string `json:"path,omitempty"`
	Query     string `json:"query,omitempty"`
	Headers   Header `json:"headers,omitempty"`
	BodyB64   string `json:"body_b64,omitempty"`

	StatusCode int    `json:"status_code,omitempty"`
	Status     string `json:"status,omitempty"`
	Error      string `json:"error,omitempty"`
}

type Header map[string][]string

func HeaderFromHTTP(h http.Header) Header {
	out := make(Header, len(h))
	for k, v := range h {
		vv := make([]string, len(v))
		copy(vv, v)
		out[k] = vv
	}
	return out
}

func (h Header) ToHTTP() http.Header {
	out := make(http.Header, len(h))
	for k, v := range h {
		vv := make([]string, len(v))
		copy(vv, v)
		out[k] = vv
	}
	return out
}

func EncodeBase64(b []byte) string          { return base64.StdEncoding.EncodeToString(b) }
func DecodeBase64(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }
