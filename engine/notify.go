package engine

import "sync"

// notifier wakes long-poll Receivers when a queue may have newly-visible work
// (Send, Abandon, scheduled activation, reaper requeue). Single-process only —
// exactly what mqlite targets, and far more precise than cross-node polling.
//
// Wake-by-close pattern: a waiter takes the current channel, re-checks the
// queue, then selects on the channel; notify() closes+rotates it, waking all.
type notifier struct {
	mu    sync.Mutex
	chans map[string]*notification
}

type notification struct {
	ch      chan struct{}
	waiters int
}

func newNotifier() *notifier { return &notifier{chans: map[string]*notification{}} }

// subscribe registers a waiter before its queue recheck. release must run on every
// exit, including timeout and cancellation. An old release cannot remove a newer
// registration installed after notify closed the previous channel.
func (n *notifier) subscribe(queue string) (<-chan struct{}, func()) {
	n.mu.Lock()
	entry, ok := n.chans[queue]
	if !ok {
		entry = &notification{ch: make(chan struct{})}
		n.chans[queue] = entry
	}
	entry.waiters++
	n.mu.Unlock()
	released := false
	return entry.ch, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if released {
			return
		}
		released = true
		entry.waiters--
		if entry.waiters == 0 && n.chans[queue] == entry {
			delete(n.chans, queue)
		}
	}
}

// notify wakes everyone currently waiting on the queue.
func (n *notifier) notify(queue string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if entry, ok := n.chans[queue]; ok {
		close(entry.ch)
		delete(n.chans, queue)
	}
}
