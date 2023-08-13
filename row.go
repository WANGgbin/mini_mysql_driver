package mysql

import (
	"database/sql/driver"
	"io"
)

var _ driver.Rows = (*rows)(nil)

type rows struct {
	result *BinaryResultSet
	stmt   *stmt
	mc     *mysqlConn

	closed bool
	err error
}

func (r *rows) Columns() []string {
	names := make([]string, r.result.cnt.ColCount)

	for idx := 0; idx < len(names); idx++ {
		//TODO: 是 Name 还是 OrgName，暂时使用 Name
		names[idx] = r.result.cols[idx].Name
	}

	return names
}

func (r *rows) Close() error {
	if r.closed {
		return nil
	}
	// Next() 发生非 io.EOF 错误，直接关闭连接
	if r.err != nil {
		return r.mc.Close()
	}
	// 读取剩下数据
	discard := make([]driver.Value, len(r.stmt.Cols))
	for {
		err := r.Next(discard)
		if err == io.EOF {
			break
		}
		if err != nil {
			return r.mc.Close()
		}
	}
	return r.close()
}

func (r *rows) close() error {
	r.closed = true
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	err := r.mc.prw.readNextRow(dest, r.stmt.Cols)
	// 如果读取完所有记录，则关闭 rows
	if err == io.EOF {
		_ = r.close()
	}
	r.err = err
	return err
}

var _ driver.Result = (*result)(nil)
type result struct {
	affectedRows int64
	lastInsertId int64
}

func (r *result) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r *result) RowsAffected() (int64, error) {
	return r.affectedRows, nil
}