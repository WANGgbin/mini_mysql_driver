package mysql

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type dbCfg struct {
	// path
	*dbCfgPath
	// query
	*dbCfgQuery
}

type dbCfgPath struct {
	ip       net.IP
	port     uint16
	protocol string
	user     string
	password string
	dbName   string
}

type dbCfgQuery struct {
	loc                  *time.Location
	charset              string
	charsetNum           uint8
	collation            string
	timeout              time.Duration
	writeTimeout         time.Duration
	readTimeout          time.Duration
	allowNativePasswords bool
	parseTime            bool
}

func parseDsn(dsn string) (*dbCfg, error) {
	var path, query string
	idx := strings.LastIndex(dsn, "?")
	if idx != -1 {
		path = dsn[:idx]
		query = dsn[idx+1:]
	} else {
		path = dsn
	}

	pathCfg, err := parseDsnPath(path)
	if err != nil {
		return nil, err
	}

	queryCfg, err := parseDsnQuery(query)
	if err != nil {
		return nil, err
	}

	return &dbCfg{
		dbCfgPath:  pathCfg,
		dbCfgQuery: queryCfg,
	}, nil
}

// parseDsn 解析 dsn 字符串，dsn 格式："user:password@protocol(ip:port)/dbName?key1=val1&key2=val2"
func parseDsnPath(path string) (*dbCfgPath, error) {
	ret := &dbCfgPath{}
	var left, right int
	for ; right < len(path); right++ {
		switch path[right] {
		case ':':
			part := path[left:right]
			if ret.user == "" {
				if part == "" {
					return nil, errors.New("miss user field")
				}
				ret.user = part
			} else {
				if part == "" {
					return nil, errors.New("miss ip field")
				}

				ret.ip = net.ParseIP(part)
				if ret.ip == nil {
					return nil, errors.New("invalid ip address")
				}
			}
			left = right + 1
		case '@':
			part := path[left:right]
			if part == "" {
				return nil, errors.New("miss password field")
			}
			ret.password = part
			left = right + 1
		case '(':
			part := path[left:right]
			if part == "" {
				return nil, errors.New("miss protocol field")
			}
			switch part {
			// 目前仅支持 tcp/unix 协议
			case "tcp", "unix":
			default:
				return nil, fmt.Errorf("unknown protocol: %s. should be one of tcp/unix", part)
			}
			ret.protocol = part
			left = right + 1
		case ')':
			part := path[left:right]
			port, err := strconv.ParseUint(part, 10, 16)
			if err != nil {
				return nil, fmt.Errorf("%s is not invalid port, should be in [0, 2^16 - 1]", part)
			}
			ret.port = uint16(port)
			left = right + 1
		case '/':
			dbName := path[right+1:]
			if len(dbName) == 0 {
				return nil, errors.New("miss dbName field")
			}
			ret.dbName = dbName
		}
	}

	err := ret.validate()
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (d *dbCfgPath) validate() error {
	val := reflect.ValueOf(d).Elem()
	for idx := 0; idx < val.NumField(); idx++ {
		field := val.Field(idx)
		if field.IsZero() {
			return fmt.Errorf("miss field: %s", val.Type().Field(idx).Name)
		}
	}
	return nil
}

const (
	allowNativePasswords = "allowNativePasswords"
	charset              = "charset"
	collation            = "collation"
	loc                  = "loc"
	parseTime            = "parseTime"
	timeout              = "timeout"
	readTimeout          = "readTimeout"
	writeTimeout         = "writeTimeout"
)

//parseDsnQuery 解析 dsn 中的 key/value 参数
func parseDsnQuery(query string) (*dbCfgQuery, error) {
	cfg := &dbCfgQuery{
		loc:                  time.Local,
		charset:              "utf8mb4",
		charsetNum:           45,
		collation:            "utf8mb4_general_ci",
		allowNativePasswords: true,
		parseTime:            true,
	}

	params, err := url.ParseQuery(query)
	if err != nil {
		return nil, err
	}

	for key, vals := range params {
		Assert(len(vals) == 1, fmt.Sprintf("cfg same %s %d times", key, len(vals)))
		val := vals[0]
		switch key {
		case allowNativePasswords:
			cfg.allowNativePasswords, err = strconv.ParseBool(val)
			if err != nil {
				return nil, fmt.Errorf("parseBool(key: %s, val: %s) error: %v", key, val, err)
			}
		case charset:
			if num, valid := charset2Num[val]; valid {
				cfg.charset = val
				cfg.charsetNum = num
			} else {
				return nil, fmt.Errorf("charset %s is invalid", val)
			}
		case collation:
			cfg.collation = val
		case loc:
			if val == "Local" {
				cfg.loc = time.Local
			} else if val == "UTC" {
				cfg.loc = time.UTC
			} else {
				return nil, fmt.Errorf("loc must be either Local or UTC, but got %s", val)
			}
		case parseTime:
			cfg.parseTime, err = strconv.ParseBool(val)
			if err != nil {
				return nil, fmt.Errorf("parseBool(key: %s, val: %s) error: %v", key, val, err)
			}
		case timeout:
			cfg.timeout, err = time.ParseDuration(val)
			if err != nil {
				return nil, fmt.Errorf("parseDuration(key: %s, val: %s) error: %v", key, val, err)
			}
		case readTimeout:
			cfg.readTimeout, err = time.ParseDuration(val)
			if err != nil {
				return nil, fmt.Errorf("parseDuration(key: %s, val: %s) error: %v", key, val, err)
			}
		case writeTimeout:
			cfg.writeTimeout, err = time.ParseDuration(val)
			if err != nil {
				return nil, fmt.Errorf("parseDuration(key: %s, val: %s) error: %v", key, val, err)
			}
		default:
			// 按需支持 key
			return nil, fmt.Errorf("unknown key: %s", key)
		}
	}

	return cfg, nil
}
