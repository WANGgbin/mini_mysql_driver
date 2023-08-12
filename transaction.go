package mysql

import "database/sql/driver"

var _ driver.Tx = (*tx)(nil)

type tx struct {
	conn *mysqlConn
}

func (t *tx) Commit() error {
	return t.conn.prw.execCmdQuery("COMMIT")
}

func (t *tx) Rollback() error {
	return t.conn.prw.execCmdQuery("ROLLBACK")
}