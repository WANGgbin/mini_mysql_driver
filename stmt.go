package mysql

import (
	"database/sql/driver"
)

type stmt struct {
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return 0
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, nil
}
