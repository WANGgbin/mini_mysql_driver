# 数据类型

go 与 mysql 的数据类型之间是如何转化的？

- sql 占位符 go -> mysql

    - 所有 int -> int64
    - 所有 uint -> uint64
    - 所有 float -> double
    - time.Time -> string
    - string -> string
    - []byte -> string
    - bool -> tiny int
  
- 查询结果 mysql -> go

    - VARCHAR/VARBINARY -> FIELD_TYPE_VAR_STRING
    - CHAR/BINARY -> FIELD_TYPE_STRING
    - BOOL -> FIELD_TYPE_TINY
    - ENUM -> FIELD_TYPE_STRING (长度为最大枚举字符串长度)

- mysql NULL 值

核心原则是能够区分 `NULL` 和 0 值。

对于基础类型：int、float 这些，要区分只能通过指针。因此，如果某个字段是可以为 NULL 的， 那么在 go 结构体的定义中，必须定义为指针。

而对于 []byte 类型，nil 和 长度为 0 是可以区分的，因此在结构体中，可以不用定义为指针。
