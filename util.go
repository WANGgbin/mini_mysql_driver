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

type Bitmap []byte

// IsSet 调用者保证 pos 是正确的
func (b *Bitmap) IsSet(pos int) bool {
	return (*b)[pos>>3] & (1 << (pos & 0x07)) != 0
}

// Set 调用者保证 pos 是正确的
func (b *Bitmap) Set(pos int) {
	(*b)[pos>>3] |= 1 << (pos & 0x07)
}

// Clear 调用者保证 pos 是正确的
func (b *Bitmap) Clear(pos int) {
	(*b)[pos>>3] &= ^(1 << (pos & 0x07))
}
