/*
 * @Author: jinde.zgm
 * @Date: 2020-09-03 23:11:41
 * @Descripttion:
 */

package etcdmap

// EventHandler can handle notifications for events that happen to a value.
// OnAdd is called when a value is added.
// OnUpdate is called when a value is modified. Not that oldVal is the last known state of the value.
// OnDelete will get the final state of the value if th is known.
type EventHandler interface {
	OnAdd(key string, value interface{})
	OnUpdate(key string, oldVal, newVal interface{})
	OnDelete(key string, value interface{})
}

// handler extends EventHandler, which receives events from etcd watcher and then calls EventHandler.
type handler struct {
	EventHandler               // Extend EventHandler
	eventc       chan *event   // Event channel
	done         chan struct{} // Handler closed signal.
}

// newHandler create handler.
func newHandler(eh EventHandler) *handler {
	return &handler{
		EventHandler: eh,
		eventc:       make(chan *event, 100),
		done:         make(chan struct{}),
	}
}

// run receive event and call EventHandler
func (h *handler) run(stopc chan struct{}) {
	defer close(h.done)
	for {
		select {
		// Handle evetn depend on event type.
		case e := <-h.eventc:
			if e.isCreated {
				h.OnAdd(e.key, e.value)
			} else if e.isDeleted {
				h.OnDelete(e.key, e.preValue)
			} else {
				h.OnUpdate(e.key, e.preValue, e.value)
			}
		// Stop signal.
		case <-stopc:
			return
		}
	}
}
