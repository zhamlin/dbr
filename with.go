package dbr

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v4"
)

// WithStmt builds `WITH ...`.
type WithStmt struct {
	runner
	EventReceiver
	Dialect

	raw

	IsRecursive bool

	Statements       []namedBuilder
	PrimaryStatement Builder

	comments Comments
}

type namedBuilder struct {
	Builder
	Name string
}

type WithBuilder = WithStmt

var (
	spaces       = strings.Repeat(" ", 2)
	doubleSpaces = spaces + spaces
)

func (b *WithStmt) Build(d Dialect, buf Buffer) error {
	if b.raw.Query != "" {
		return b.raw.Build(d, buf)
	}

	err := b.comments.Build(d, buf)
	if err != nil {
		return err
	}

	buf.WriteString("WITH ")
	if b.IsRecursive {
		buf.WriteString("RECURSIVE ")
	}

	buf.WriteString("\n" + spaces)
	for i, builder := range b.Statements {
		if i > 0 {
			buf.WriteString(",\n" + spaces)
		}
		buf.WriteString(builder.Name + " as (\n" + doubleSpaces)
		if err := builder.Build(b.Dialect, buf); err != nil {
			return err
		}
		buf.WriteString("\n" + spaces + ")")
	}
	if b.PrimaryStatement != nil {
		buf.WriteString("\n")
		if err := b.PrimaryStatement.Build(b.Dialect, buf); err != nil {
			return err
		}
	}
	return nil
}

func With() *WithStmt {
	return &WithStmt{}
}

// With creates a WithStmt.
func (sess *Session) With() *WithStmt {
	b := With()
	b.runner = sess
	b.EventReceiver = sess.EventReceiver
	b.Dialect = sess.Dialect
	return b
}

// With creates an WithStmt.
func (tx *Tx) With() *WithStmt {
	b := With()
	b.runner = tx
	b.EventReceiver = tx.EventReceiver
	b.Dialect = tx.Dialect
	return b
}

// WithBySql creates an WithStmt from raw query.
func WithBySql(query string, value ...interface{}) *WithStmt {
	return &WithStmt{
		raw: raw{
			Query: query,
			Value: value,
		},
	}
}

func (b *WithStmt) Recursive(v bool) *WithStmt {
	b.IsRecursive = v
	return b
}

func (b *WithStmt) As(name string, statement Builder) *WithStmt {
	b.Statements = append(b.Statements, namedBuilder{
		Name:    name,
		Builder: statement,
	})
	return b
}

func (b *WithStmt) Primary(query Builder) *WithStmt {
	b.PrimaryStatement = query
	return b
}

// Rows executes the query and returns the rows returned, or any error encountered.
func (b *WithStmt) Rows() (pgx.Rows, error) {
	return b.RowsContext(context.Background())
}

func (b *WithStmt) RowsContext(ctx context.Context) (pgx.Rows, error) {
	_, rows, err := queryRows(ctx, b.runner, b.EventReceiver, b, b.Dialect)
	return rows, err
}

func (b *WithStmt) LoadOneContext(ctx context.Context, value interface{}) error {
	count, err := query(ctx, b.runner, b.EventReceiver, b, b.Dialect, value)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

// LoadOne loads SQL result into go variable that is not a slice.
// Unlike Load, it returns ErrNotFound if the SQL result row count is 0.
//
// See https://godoc.org/github.com/gocraft/dbr#Load.
func (b *WithStmt) LoadOne(value interface{}) error {
	return b.LoadOneContext(context.Background(), value)
}

func (b *WithStmt) LoadContext(ctx context.Context, value interface{}) (int, error) {
	return query(ctx, b.runner, b.EventReceiver, b, b.Dialect, value)
}

// Load loads multi-row SQL result into a slice of go variables.
//
// See https://godoc.org/github.com/gocraft/dbr#Load.
func (b *WithStmt) Load(value interface{}) (int, error) {
	return b.LoadContext(context.Background(), value)
}

// Iterate executes the query and returns the Iterator, or any error encountered.
func (b *WithStmt) Iterate() (Iterator, error) {
	return b.IterateContext(context.Background())
}

// IterateContext executes the query and returns the Iterator, or any error encountered.
func (b *WithStmt) IterateContext(ctx context.Context) (Iterator, error) {
	_, rows, err := queryRows(ctx, b.runner, b.EventReceiver, b, b.Dialect)
	if err != nil {
		if rows != nil {
			rows.Close()
		}
		return nil, err
	}
	columns := rowColumns(rows)
	iterator := iteratorInternals{
		rows:    rows,
		columns: columns,
	}
	return &iterator, err
}
