package mysql

import "database/sql/driver"

var _ driver.Tx = (*tx)(nil)

type tx struct {
	conn *mysqlConn
}

func (t *tx) Commit() error {
	if t.conn.isClosed() {
		return ErrConnHasBeenClosed
	}
	err := t.conn.prw.execCmdQuery("COMMIT")
	if err != nil {
		_ = t.conn.Close()
	}
	return err
}

func (t *tx) Rollback() error {
	if t.conn.isClosed() {
		return ErrConnHasBeenClosed
	}
	err := t.conn.prw.execCmdQuery("ROLLBACK")
	if err != nil {
		_ = t.conn.Close()
	}
	return err
}