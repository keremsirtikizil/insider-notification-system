package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
	"github.com/keremsirtikizil/insider-notification-system/internal/events"
	"github.com/keremsirtikizil/insider-notification-system/internal/observability"
)

const (
	wsSendBuffer   = 256
	wsWriteTimeout = 5 * time.Second
	wsPingInterval = 30 * time.Second
)

// Hub fans status events out to WebSocket subscribers. Each subscriber may
// filter by notification, batch and/or channel.
type Hub struct {
	mu      sync.RWMutex
	clients map[*wsClient]struct{}
	logger  *slog.Logger
	metrics *observability.Metrics
	done    chan struct{}
	once    sync.Once
}

type wsClient struct {
	conn           *websocket.Conn
	send           chan events.Event
	notificationID *uuid.UUID
	batchID        *uuid.UUID
	channel        string
}

func (c *wsClient) matches(e events.Event) bool {
	if c.notificationID != nil && *c.notificationID != e.NotificationID {
		return false
	}
	if c.batchID != nil && (e.BatchID == nil || *c.batchID != *e.BatchID) {
		return false
	}
	return c.channel == "" || c.channel == e.Channel
}

// NewHub creates an empty hub.
func NewHub(logger *slog.Logger, metrics *observability.Metrics) *Hub {
	return &Hub{clients: map[*wsClient]struct{}{}, logger: logger, metrics: metrics, done: make(chan struct{})}
}

// Run broadcasts events until the stream ends or ctx is cancelled, then
// disconnects every client.
func (h *Hub) Run(ctx context.Context, in <-chan events.Event) {
	defer h.shutdown()
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-in:
			if !ok {
				return
			}
			h.Broadcast(e)
		}
	}
}

// Broadcast delivers e to every matching client without blocking. A client
// whose buffer is full is disconnected rather than slowing everyone down.
func (h *Hub) Broadcast(e events.Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if !c.matches(e) {
			continue
		}
		select {
		case c.send <- e:
		default:
			go c.conn.Close(websocket.StatusPolicyViolation, "client too slow")
		}
	}
}

func (h *Hub) shutdown() {
	h.once.Do(func() {
		close(h.done)
		h.mu.RLock()
		defer h.mu.RUnlock()
		for c := range h.clients {
			go c.conn.Close(websocket.StatusGoingAway, "server shutting down")
		}
	})
}

func (h *Hub) add(c *wsClient) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	if h.metrics != nil {
		h.metrics.WebSocketClients.Inc()
	}
}

func (h *Hub) remove(c *wsClient) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
	if h.metrics != nil {
		h.metrics.WebSocketClients.Dec()
	}
}

// ServeWS handles GET /v1/ws?notification_id=&batch_id=&channel=
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	c := &wsClient{send: make(chan events.Event, wsSendBuffer)}
	ve := &domain.ValidationError{}
	if v := q.Get("notification_id"); v != "" {
		if id, err := uuid.Parse(v); err != nil {
			ve.Add("notification_id", "must be a UUID")
		} else {
			c.notificationID = &id
		}
	}
	if v := q.Get("batch_id"); v != "" {
		if id, err := uuid.Parse(v); err != nil {
			ve.Add("batch_id", "must be a UUID")
		} else {
			c.batchID = &id
		}
	}
	if v := q.Get("channel"); v != "" {
		if ch, err := domain.ParseChannel(v); err != nil {
			ve.Add("channel", "%s", err.Error())
		} else {
			c.channel = string(ch)
		}
	}
	if ve.Err() != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_query", "invalid subscription filters", ve.Fields)
		return
	}

	// The connection outlives the server's per-request read/write deadlines.
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	_ = rc.SetWriteDeadline(time.Time{})

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
	if err != nil {
		return // Accept already wrote the HTTP error
	}
	c.conn = conn
	defer func() { _ = conn.CloseNow() }()
	h.add(c)
	defer h.remove(c)

	// CloseRead consumes control frames and cancels ctx when the peer leaves.
	ctx := conn.CloseRead(context.WithoutCancel(r.Context()))
	ping := time.NewTicker(wsPingInterval)
	defer ping.Stop()

	if err := h.write(ctx, conn, map[string]any{
		"type": "subscribed",
		"filters": map[string]any{
			"notification_id": c.notificationID, "batch_id": c.batchID, "channel": c.channel,
		},
	}); err != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.done:
			return
		case e := <-c.send:
			if err := h.write(ctx, conn, e); err != nil {
				return
			}
		case <-ping.C:
			pctx, cancel := context.WithTimeout(ctx, wsWriteTimeout)
			err := conn.Ping(pctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func (h *Hub) write(ctx context.Context, conn *websocket.Conn, v any) error {
	ctx, cancel := context.WithTimeout(ctx, wsWriteTimeout)
	defer cancel()
	return wsjson.Write(ctx, conn, v)
}
