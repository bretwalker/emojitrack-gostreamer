package sseserver

import (
	. "github.com/azer/debug"
)

type hub struct {
	broadcast   chan SSEMessage      // Inbound messages to propogate out.
	connections map[*connection]bool // Registered connections.
	register    chan *connection     // Register requests from the connections.
	unregister  chan *connection     // Unregister requests from connections.
}

func newHub() *hub {
	return &hub{
		broadcast:   make(chan SSEMessage),
		connections: make(map[*connection]bool),
		register:    make(chan *connection),
		unregister:  make(chan *connection),
	}
}

func (h *hub) run() {
	for {
		select {
		case c := <-h.register:
			Debug("new connection being registered for " + c.namespace)
			h.connections[c] = true
		case c := <-h.unregister:
			Debug("connection told us to unregister for " + c.namespace)
			delete(h.connections, c)
			close(c.send)
		case msg := <-h.broadcast:
			for c := range h.connections {
				if msg.Namespace == c.namespace {
					select {
					case c.send <- msg.sseFormat():
					default:
						Debug("cant pass to a connection send chan, buffer is full -- kill it with fire")
						delete(h.connections, c)
						close(c.send)
						// go c.ws.Close()
						/* TODO: figure out what to do here...
						we are already closing the send channel, in *theory* shouldn't the
						connection clean up? I guess possible it doesnt if its deadlocked or
						something... is it?

						we want to make sure to always close the HTTP connection though,
						so server can never fill up max num of open sockets.
						*/
					}
				}
			}
		}
	}
}
