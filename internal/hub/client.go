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
		_ = c.conn.Close()
	})
}

func (c *Client) TrySend(msg []byte) {
	select {
	case <-c.closed:
		return
	default:
	}
	select {
	case c.send <- msg:
	default:
		// drop on backpressure
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

