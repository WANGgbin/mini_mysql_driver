package mysql

import (
	"fmt"
	"reflect"
	"unsafe"
)

func Assert(exp bool, format string, args ...interface{}) {
	if !exp {
		panic(fmt.Sprintf(format, args...))
	}
}

func Str2Byte(s string) []byte {
	strHeader := (*reflect.StringHeader)(unsafe.Pointer(&s))
	sliceHeader := reflect.SliceHeader{
		Data: strHeader.Data,
		Len:  strHeader.Len,
		Cap:  strHeader.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&sliceHeader))
}

func Byte2Str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
