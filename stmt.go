package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
)

var _ driver.Stmt = (*stmt)(nil)
var _ driver.NamedValueChecker = (*stmt)(nil)
var _ driver.StmtQueryContext = (*stmt)(nil)
var _ driver.StmtExecContext = (*stmt)(nil)

type stmt struct {
	okResp *StmtPrepareOK
	Cols   []*ColumnDef41
	Params []*ColumnDef41

	mc *mysqlConn
	closed bool
}

func (s *stmt) Close() error {
	// 在关闭 rows 的时候，可能直接关闭连接。sql 包在关闭 rows 之后，还会关闭 stmt，因此这里加一层判断
	// 如果 mc 已经关闭，直接返回
	if s.mc.isClosed() {
		return nil
	}
	if s.closed {
		return nil
	}
	s.closed = true
	scPkt := &StmtClose{
		Status: 0x19,
		StmtID: s.okResp.StmtID,
	}
	s.mc.curSeqID = 0
	err := s.mc.prw.write(scPkt, s.mc.curSeqID)
	if err != nil {
		_ = s.mc.Close()
		return err
	}
	return nil
}

func (s *stmt) NumInput() int {
	return int(s.okResp.NumParams)
}


func (s *stmt) QueryContext(ctx context.Context, nvargs []driver.NamedValue) (driver.Rows, error) {
	args, err := namedValuesToValues(nvargs)
	if err != nil {
		_ = s.Close()
		return nil, err
	}

	if ctx.Done() != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	return s.Query(args)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	err := s.mc.prw.writeStmtExecPkt(s.okResp, args)
	if err != nil {
		return nil, s.handleWriteError(err)
	}

	result, err := s.mc.prw.readQueryResp()
	if err != nil {
		return nil, s.handleReadError(err)
	}

	return &rows{
		result: result,
		stmt:   s,
		mc:     s.mc,
	}, nil
}

func (s *stmt) ExecContext(ctx context.Context, nvargs []driver.NamedValue) (driver.Result, error) {
	args, err := namedValuesToValues(nvargs)
	if err != nil {
		_ = s.Close()
		return nil, err
	}

	if ctx.Done() != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	return s.Exec(args)
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	err := s.mc.prw.writeStmtExecPkt(s.okResp, args)
	if err != nil {
		return nil, s.handleWriteError(err)
	}

	okPkt, err := s.mc.prw.readOkPkt("StmtExec")
	if err != nil {
		return nil, s.handleReadError(err)
	}

	return &result{
		affectedRows: int64(okPkt.AffectedRows),
		lastInsertId: int64(okPkt.LastInsertId),
	}, nil
}

func (s *stmt) CheckNamedValue(arg *driver.NamedValue) (err error) {
	arg.Value, err = convert(arg.Value)
	return
}

func convert(src interface{}) (interface{}, error) {
	if driver.IsValue(src) {
		return src, nil
	}

	var ret interface{}
	val := reflect.ValueOf(src)
	switch val.Kind() {
	case reflect.Ptr:
		// 空指针对应 nil interface{} 即 mysql 中的 NULL
		if val.IsNil() {
			return nil, nil
		}
		// 注意：这里一定要通过 Interface() 函数将 reflect.Value 转化为 interface{}
		// 不能直接：var i interface{} = reflect.Value
		return convert(val.Elem().Interface())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		ret = val.Uint()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		ret = val.Int()
	case reflect.Bool:
		ret = val.Bool()
	case reflect.Float32, reflect.Float64:
		ret = val.Float()
	case reflect.String:
		ret = val.String()
	case reflect.Slice:
		elemTyp := val.Type().Elem()
		switch elemTyp.Kind() {
		case reflect.Uint8:
			// 注意：val.Bytes() 并没有分配新的空间，直接返回底层 []byte 的引用
			ret = val.Bytes()
		default:
			return nil, fmt.Errorf("type of slice must be reflect.Uint8, but we got %s", elemTyp.Kind().String())
		}
	}
	return ret, nil
}

func namedValuesToValues(nvargs []driver.NamedValue) ([]driver.Value, error) {
	args := make([]driver.Value, len(nvargs))

	for idx, nvarg := range nvargs {
		if nvarg.Name != "" {
			return nil, errors.New("sql: driver doesn't support the use of Named Parameters")
		}
		args[idx] = nvarg.Value
	}

	return args, nil
}

// handleWriteError 是否需要重试
// tcp 连接可能是从连接池中拿到的旧连接，很有可能该链接已经关闭，因此我们需要区分这种类型的错误(writeZeroBytes)，
// 并在这种错误发生的时候，通过返回 driver.ErrBadConn 的方式通过 sql 包进行重试。
// 需要注意的是 driver.ErrBadConn 只在非 tx 中生效
func (s *stmt) handleWriteError(err error) error {
	writeErr, ok := err.(*ErrorReadWritePkt)
	if !ok {
		_ = s.mc.Close()
		return err
	}

	if writeErr.errType == WriteErrTypeMarshalError {
		_ = s.Close()
		return err
	}

	_ = s.mc.Close()

	if writeErr.errType == WriteErrTypeWriteZeroBytes {
		// 通知 sql 包进行重试
		return driver.ErrBadConn
	}
	return err
}

// handleReadError 是否可重用连接
func (s *stmt) handleReadError(err error) error {
	if readErr, ok := err.(*ErrorReadWritePkt); ok {
		if readErr.errType == ReadErrTypeErrPkt {
			_ = s.Close()
			return err
		}
	}

	_ = s.mc.Close()
	return err
}