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
	chans map[string]chan struct{}
}

func newNotifier() *notifier { return &notifier{chans: map[string]chan struct{}{}} }

// wait returns the current wake channel for a queue. Take it BEFORE re-checking
// the queue so a Send racing in between cannot be missed.
func (n *notifier) wait(queue string) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	ch, ok := n.chans[queue]
	if !ok {
		ch = make(chan struct{})
		n.chans[queue] = ch
	}
	return ch
}

// notify wakes everyone currently waiting on the queue.
func (n *notifier) notify(queue string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ch, ok := n.chans[queue]; ok {
		close(ch)
		delete(n.chans, queue)
	}
}
