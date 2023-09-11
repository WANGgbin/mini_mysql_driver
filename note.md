# 数据类型

go 与 mysql 的数据类型之间是如何转化的？

- sql 占位符
  
  两次数据类型转化：
    
  - 第一次类型转化
    
    go 类型 -> 占位符类型(int64/uint64/float64/bool/[]byte/string/time.Time/nil)
    > 占位符类型只能为这 8 种。
    
    自定义类型可通过实现 `driver.Valuer` 接口转化为 8 中占位符类型的一种。
    
    ```go
    type Valuer interface {
	    // Value returns a driver Value.
	    // Value must not panic.
	    Value() (Value, error)
    }
    ```
    
  - 第二次类型转化
    
    占位符类型 -> mysql 类型
    
    需要注意的是，[]byte/string/time.Time 类型都被转化为 FIELD_TYPE_STRING 类型。
    - 所有 int -> int64
    - 所有 uint -> uint64
    - 所有 float -> double
    - time.Time -> string
    - string -> string
    - []byte -> string
    - bool -> tiny int
  
- 查询结果 
   
    获取查询结果的时候，同样也存在两次数据类型转化。
    
    - mysql -> 中间类型
    
      - mysql 类型在协议中的类型映射如下：
        - VARCHAR/VARBINARY -> FIELD_TYPE_VAR_STRING
        - CHAR/BINARY -> FIELD_TYPE_STRING
        - BOOL -> FIELD_TYPE_TINY
        - ENUM -> FIELD_TYPE_STRING (长度为最大枚举字符串长度)
      
      - 协议类型到中间类型映射如下：
      
        - 不管是二进制/字符串，不管定长/变长，统统转化为字符串类型。（取决于 driver 实现）
        - float -> float32
        - double -> float64
        - 所有整型 -> int64
  
    - 中间类型 -> model 中具体类型
  
    自定义类型可以实现 `driver.Scanner` 接口，将中间类型转化为自定义类型。接口定义如下：
    ```go
    // Scanner is an interface used by Scan.
    type Scanner interface {
	// Scan assigns a value from a database driver.
	//
	// The src value will be of one of the following types:
	//
	//    int64
	//    float64
	//    bool
	//    []byte
	//    string
	//    time.Time
	//    nil - for NULL values
	//
	// An error should be returned if the value cannot be stored
	// without loss of information.
	//
	// Reference types such as []byte are only valid until the next call to Scan
	// and should not be retained. Their underlying memory is owned by the driver.
	// If retention is necessary, copy their values before the next call to Scan.
    Scan(src interface{}) error
  }
    ```
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