package engine

import (
	"context"
	"database/sql"
)

func affected(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLockLost // wrong/expired token, or already settled
	}
	return nil
}

// Complete removes a successfully-processed message (fencing on lock_token).
func (e *Engine) Complete(ctx context.Context, queue string, seq int64, token string) error {
	res, err := e.db.exec(ctx,
		`DELETE FROM messages WHERE id=? AND queue=? AND lock_token=?`, seq, queue, token)
	if err != nil {
		return err
	}
	return affected(res)
}

// Abandon releases the lock and redelivers, or dead-letters if delivery_count
// has reached the queue's max (§8.5). delayMs applies exponential-backoff style
// re-visibility when requeued.
func (e *Engine) Abandon(ctx context.Context, queue string, seq int64, token string, delayMs int64) error {
	now := e.now()
	res, err := e.db.exec(ctx, `
		UPDATE messages SET
		    state = CASE WHEN delivery_count >= (SELECT max_delivery_count FROM queues WHERE name=messages.queue)
		                 THEN 'dead_lettered' ELSE 'active' END,
		    locked_until = 0,
		    lock_token   = NULL,
		    visible_at = CASE WHEN delivery_count >= (SELECT max_delivery_count FROM queues WHERE name=messages.queue)
		                      THEN visible_at ELSE ? END,
		    dead_letter_reason = CASE WHEN delivery_count >= (SELECT max_delivery_count FROM queues WHERE name=messages.queue)
		                              THEN 'MaxDeliveryCountExceeded' ELSE dead_letter_reason END
		 WHERE id=? AND queue=? AND lock_token=?`,
		now+delayMs, seq, queue, token)
	if err != nil {
		return err
	}
	if err := affected(res); err != nil {
		return err
	}
	e.note.notify(queue)
	return nil
}

// DeadLetter moves a locked message to the dead-letter state with a reason.
func (e *Engine) DeadLetter(ctx context.Context, queue string, seq int64, token, reason, desc string) error {
	if reason == "" {
		reason = ReasonAppRequested
	}
	res, err := e.db.exec(ctx, `
		UPDATE messages SET state='dead_lettered', locked_until=0, lock_token=NULL,
		    dead_letter_reason=?, dead_letter_description=?
		 WHERE id=? AND queue=? AND lock_token=?`,
		reason, nz(desc), seq, queue, token)
	if err != nil {
		return err
	}
	return affected(res)
}

// Defer sets a message aside; it is later retrieved by seq via ReceiveDeferred.
func (e *Engine) Defer(ctx context.Context, queue string, seq int64, token string) error {
	res, err := e.db.exec(ctx,
		`UPDATE messages SET state='deferred', locked_until=0, lock_token=NULL
		   WHERE id=? AND queue=? AND lock_token=?`, seq, queue, token)
	if err != nil {
		return err
	}
	return affected(res)
}

// RenewLock extends the lock lease by the queue's lock duration (§11.3).
func (e *Engine) RenewLock(ctx context.Context, queue string, seq int64, token string) error {
	q, err := e.loadQueue(ctx, queue)
	if err != nil {
		return err
	}
	res, err := e.db.exec(ctx,
		`UPDATE messages SET locked_until=? WHERE id=? AND queue=? AND lock_token=?`,
		e.now()+q.lockDurationMs, seq, queue, token)
	if err != nil {
		return err
	}
	return affected(res)
}
