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

# 执行 SQL 流程

事务与非事务的读写不尽相同，这里我们重点讨论非事务的读写。

## 如何执行 SQL
执行 SQL 有两种方式：
  - 通过 CmdQuery 直接执行
    - 优点
      执行一次，效率快
    - 缺点
      SQL 注入风险
  - 通过 Statement 执行
    - 优点
      防 SQL 注入
    - 缺点
      需要 prepare、exec、close 三个 round trip，性能较差
      
## 具体流程

- 首先从连接池获取一条空闲的连接
- 发送数据包
- 发生错误
  - 需要衡量，是否可以重新执行该操作，如果可以，返回 `driver.ErrBadConn`，告诉 sql 包，获取新的连接重新执行该操作。
  - 其他错误，直接返回
    
本质上，对于写操作，需要注意重试的问题。对于读操作，需要注意 errPkt 情况下，连接复用的问题。

# 错误处理

需要注意，什么时候应该关闭连接、什么时候关闭 stmt、什么时候关闭 row、什么时候关闭 tx

# 事务

事务执行器件都是跟一个 tcp 连接绑定的。因此在开启事务后，写操作发生错误时，不再尝试区分 `driver.ErrBadConn` 错误。