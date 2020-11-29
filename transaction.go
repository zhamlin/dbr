package dbr

import (
	"context"
	"database/sql"
	"time"

	"github.com/jackc/pgx/v4"
)

// Tx is a transaction created by Session.
type Tx struct {
	EventReceiver
	Dialect
	pgx.Tx
	Timeout time.Duration
}

// GetTimeout returns timeout enforced in Tx.
func (tx *Tx) GetTimeout() time.Duration {
	return tx.Timeout
}

// BeginTx creates a transaction with TxOptions.
func (sess *Session) BeginTx(ctx context.Context, opts pgx.TxOptions) (*Tx, error) {
	tx, err := sess.Connection.BeginTx(ctx, opts)
	if err != nil {
		return nil, sess.EventErr("dbr.begin.error", err)
	}
	sess.Event("dbr.begin")

	return &Tx{
		EventReceiver: sess.EventReceiver,
		Dialect:       sess.Dialect,
		Tx:            tx,
		Timeout:       sess.GetTimeout(),
	}, nil
}

// Begin creates a transaction for the given session.
func (sess *Session) Begin() (*Tx, error) {
	return sess.BeginTx(context.Background(), pgx.TxOptions{})
}

// Commit finishes the transaction.
func (tx *Tx) Commit(ctx context.Context) error {
	err := tx.Tx.Commit(ctx)
	if err != nil {
		return tx.EventErr("dbr.commit.error", err)
	}
	tx.Event("dbr.commit")
	return nil
}

// Rollback cancels the transaction.
func (tx *Tx) Rollback(ctx context.Context) error {
	err := tx.Tx.Rollback(ctx)
	if err != nil {
		return tx.EventErr("dbr.rollback", err)
	}
	tx.Event("dbr.rollback")
	return nil
}

// RollbackUnlessCommitted rollsback the transaction unless
// it has already been committed or rolled back.
//
// Useful to defer tx.RollbackUnlessCommitted(), so you don't
// have to handle N failure cases.
// Keep in mind the only way to detect an error on the rollback
// is via the event log.
func (tx *Tx) RollbackUnlessCommitted(ctx context.Context) {
	err := tx.Tx.Rollback(ctx)
	if err == sql.ErrTxDone {
		// ok
	} else if err != nil {
		tx.EventErr("dbr.rollback_unless_committed", err)
	} else {
		tx.Event("dbr.rollback")
	}
}
