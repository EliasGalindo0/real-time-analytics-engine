package hub

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	conn *websocket.Conn
	send chan []byte

	closeOnce sync.Once
	closed    chan struct{}
}

func NewClient(conn *websocket.Conn, queueDepth int) *Client {
	return &Client{
		conn:   conn,
		send:   make(chan []byte, queueDepth),
		closed: make(chan struct{}),
	}
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		close(c.send)
		_ = c.conn.Close()
	})
}

// TrySend is non-blocking. Returns false if the message could not be enqueued.
func (c *Client) TrySend(msg []byte) bool {
	select {
	case <-c.closed:
		return false
	default:
	}
	select {
	case c.send <- msg:
		return true
	default:
		// drop on backpressure
		return false
	}
}

func (c *Client) WritePump(pingEvery time.Duration, writeTimeout time.Duration) {
	ticker := time.NewTicker(pingEvery)
	defer ticker.Stop()
	defer c.Close()

	for {
		select {
		case <-c.closed:
			return
		case msg, ok := <-c.send:
			if !ok {
				return
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ReadPump exists to promptly detect disconnects and refresh read deadlines.
// We ignore incoming messages; this is a "server-push" websocket.
func (c *Client) ReadPump(idleTimeout time.Duration, maxMsgBytes int64) {
	defer c.Close()

	c.conn.SetReadLimit(maxMsgBytes)
	_ = c.conn.SetReadDeadline(time.Now().Add(idleTimeout))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(idleTimeout))
		return nil
	})

	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}

