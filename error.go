package mysql

import (
	"errors"
	"fmt"
)

var (
	ErrConnHasBeenClosed = errors.New("conn has been closed")
)

// 区分这两种错误的原因是，如果没有写入任何字节，则应该返回 driver.ErrBadConn, 这样 sql 包会进行重试
const (
	WriteErrTypeWriteZeroBytes = "write zero bytes"
	WriteErrTypeWriteSocket    = "write socket"
	WriteErrTypeMarshalError   = "marshal error"
)

// 区分读错误的目的是：在非 ErrReadSocket 的时候，是可以复用底层链接的
const (
	ReadErrTypeSocket       = "read socket"
	ReadErrTypeErrPkt       = "read err pkt"
	ReadErrTypeUnknownPkt   = "read unknown pkt"
	ReadErrTypeMalformedPkt = "read malformed pkt"
)

var _ error = (*ErrorReadWritePkt)(nil)

type ErrorReadWritePkt struct {
	event   string
	errType string
	raw     error
}

func (e *ErrorReadWritePkt) Error() string {
	return fmt.Sprintf("event: %s, errType: %s, rawErr: %v", e.event, e.errType, e.raw)
}

func (e *ErrorReadWritePkt) Unwrap() error {
	return e.raw
}

