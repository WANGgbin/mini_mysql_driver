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
}

func (s *stmt) Close() error {
	scPkt := &StmtClose{
		Status: 0x19,
		StmtID: s.okResp.StmtID,
	}
	s.mc.curSeqID = 0
	err := s.mc.prw.write(scPkt, s.mc.curSeqID)
	if err != nil {
		return fmt.Errorf("close prepare statement error: %v", err)
	}
	return nil
}

func (s *stmt) NumInput() int {
	return int(s.okResp.NumParams)
}


func (s *stmt) QueryContext(ctx context.Context, nvargs []driver.NamedValue) (driver.Rows, error) {
	args, err := namedValuesToValues(nvargs)
	if err != nil {
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

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	err := s.mc.prw.writeStmtExecPkt(s.okResp, args)
	if err != nil {
		return nil, driver.ErrBadConn
	}

	result, err := s.mc.prw.readQueryResp()
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("stmt.Exec write exec pkt error: %v", err)
	}

	okPkt, err := s.mc.prw.readOkPkt()
	if err != nil {
		return nil, fmt.Errorf("stmt.Exec read ok Pkt error: %v", err)
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
		if val.IsNil() {
			return nil, errors.New("must not be nil pointer")
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
		elemVal := val.Elem()
		switch elemVal.Kind() {
		case reflect.Uint8:
			// 注意：val.Bytes() 并没有分配新的空间，直接返回底层 []byte 的引用
			ret = val.Bytes()
		default:
			return nil, fmt.Errorf("type of slice must be reflect.Uint8, but we got %s", elemVal.Kind().String())
		}
	}
	return ret, nil
}
