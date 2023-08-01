package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"time"
)

func init() {
	sql.Register("mini_mysql", &mysqlDriver{})
}

var _ driver.Driver = (*mysqlDriver)(nil)

type mysqlDriver struct{}

func (m *mysqlDriver) Open(dsn string) (driver.Conn, error) {
	dbInfo, err := parseDsn(dsn)
	if err != nil {
		return nil, fmt.Errorf("parseDsn error: %v", err)
	}

	return newMysqlConn(dbInfo)
}

type dbInfo struct {
	ip       net.IP
	port     uint16
	protocol string
	user     string
	password string
	dbName   string
}

// parseDsn 解析 dsn 字符串，dsn 格式："user:password@protocol(ip:port)/dbName?key1=val1&key2=val2"
func parseDsn(dsn string) (*dbInfo, error) {
	ret := &dbInfo{}
	var left, right int
	for ; right < len(dsn); right++ {
		switch dsn[right] {
		case ':':
			part := dsn[left:right]
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
			part := dsn[left:right]
			if part == "" {
				return nil, errors.New("miss password field")
			}
			ret.password = part
			left = right + 1
		case '(':
			part := dsn[left:right]
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
			part := dsn[left:right]
			port, err := strconv.ParseUint(part, 10, 16)
			if err != nil {
				return nil, fmt.Errorf("%s is not invalid port, should be in [0, 2^16 - 1]", part)
			}
			ret.port = uint16(port)
			left = right + 1
		case '/':
			dbName := dsn[right+1:]
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

func (d *dbInfo) validate() error {
	val := reflect.ValueOf(d).Elem()
	for idx := 0; idx < val.NumField(); idx++ {
		field := val.Field(idx)
		if field.IsZero() {
			return fmt.Errorf("miss field: %s", val.Type().Field(idx).Name)
		}
	}
	return nil
}

var _ driver.Conn = (*mysqlConn)(nil)
var _ driver.ConnPrepareContext = (*mysqlConn)(nil)
var _ driver.Pinger = (*mysqlConn)(nil)

type mysqlConn struct {
	dbInfo   *dbInfo
	conn     net.Conn
	prw      *pktReadWriter
	capFlag  CapFlag
	curSeqID uint8
	closed   bool
}

var defaultTimeOut = 1 * time.Second

func newMysqlConn(db *dbInfo) (*mysqlConn, error) {
	dialer := &net.Dialer{
		// TODO(@wangguobin): 用户可配置建立连接超时时间
		Timeout:   defaultTimeOut,
		KeepAlive: 30 * time.Minute,
	}
	conn, err := dialer.Dial(db.protocol, fmt.Sprintf("%s:%d", db.ip, db.port))
	if err != nil {
		return nil, err
	}

	mc := &mysqlConn{
		dbInfo: db,
		conn:   conn,
	}

	mc.prw = newPktReadWriter(mc)

	err = mc.login()
	if err != nil {
		_ = mc.Close()
		return nil, err
	}

	return mc, nil
}

// login 完成跟服务端的握手：能力交换、身份验证
func (m *mysqlConn) login() error {
	var hs HandshakeV10
	err := m.prw.read(&hs)
	if err != nil {
		return err
	}

	if !m.SrvSupportCpbs([]int{
		CapClientLongPassword,
		CapClientColumnLongFlag,
		CapClientProtocol41,
		CapClientConnectWithDB,
		CapClientPluginAuth,
		CapClientTransactions,
		CapClientSessionTrack,
		CapClientMultiResults,
		CapClientDeprecateEof,
		CapClientAuthentication41,
	}) {
		return errors.New("srv has no enough capabilities")
	}

	err = m.sendHandshakeResp(&hs)
	if err != nil {
		return fmt.Errorf("sendHandshakeResp() error: %v", err)
	}

	pkt, err := m.prw.readAuthResult()
	if err != nil {
		return err
	}

	if _, ok := pkt.(*OkPacket); ok {
		return nil
	}

	err = m.sendAuthSwitchResp(pkt.(*AuthSwitchReq))
	if err != nil {
		return err
	}

	pkt, err = m.prw.readAuthResult()
	if err != nil {
		return err
	}

	if _, ok := pkt.(*OkPacket); ok {
		return nil
	}

	return errors.New("after sending switch auth resp, but receive switch auth req again")
}

func (m *mysqlConn) SrvSupportCpbs(cpbs []int) bool {
	for _, cpb := range cpbs {
		if !m.capFlag.IsSet(cpb) {
			return false
		}
	}

	return true
}

func (m *mysqlConn) sendHandshakeResp(hs *HandshakeV10) error {
	resp, err := m.newHandshakeResp(hs)
	if err != nil {
		return fmt.Errorf("newHandShakeResp() error: %v", err)
	}

	err = m.prw.Write(resp, m.curSeqID)
	if err != nil {
		return err
	}

	return nil
}

func (m *mysqlConn) newHandshakeResp(hs *HandshakeV10) (*HandshakeResp41, error) {
	authResp, err := m.buildAuthResp([]byte(hs.AuthDataPartOne+hs.AuthDataPartTwo), hs.AuthPluginName)
	if err != nil {
		return nil, fmt.Errorf("auth error: %v", err)
	}

	m.capFlag = buildClientCapFlag()

	return &HandshakeResp41{
		CapFlag:       m.capFlag,
		MaxPacketSize: 0,
		CharSet:       45,
		Pad:           Byte2Str(make([]byte, 23)),
		UserName:      m.dbInfo.user,
		AuthRespLen:   uint8(len(authResp)),
		AuthResp:      Byte2Str(authResp),
		DataBase:      m.dbInfo.dbName,
		PluginName:    hs.AuthPluginName,
	}, nil
}

func (m *mysqlConn) sendAuthSwitchResp(req *AuthSwitchReq) error {
	resp, err := m.newAuthSwitchResp(req)
	if err != nil {
		return err
	}

	return m.prw.Write(resp, m.curSeqID)
}

func (m *mysqlConn) newAuthSwitchResp(req *AuthSwitchReq) (*AuthSwitchResp, error) {
	authResp, err := m.buildAuthResp(Str2Byte(req.PluginData), req.PluginName)
	if err != nil {
		return nil, err
	}

	return &AuthSwitchResp{
		AuthData: Byte2Str(authResp),
	}, nil
}

func (m *mysqlConn) buildAuthResp(scramble []byte, authMethod string) ([]byte, error) {
	switch authMethod {
	case "caching_sha2_password":
		return buildAuthRespWithCachingSha2Password(scramble, m.dbInfo.password), nil
	case "mysql_native_password":
		return buildAuthRespWithMysqlNativePassword(scramble[:20], m.dbInfo.password), nil
	default:
		return nil, fmt.Errorf("unknown authMethod: %s", authMethod)
	}
}

// Ping 发送 CmdPing 检测服务端是否存活
func (m *mysqlConn) Ping(ctx context.Context) error {
	m.curSeqID = 0
	err := m.prw.Write(&CmdPing{Name: 0x0E}, m.curSeqID)
	if err != nil {
		return err
	}

	if _, err = m.prw.readOkPkt(); err != nil {
		return err
	}

	// 判断 ctx 是否取消
	if ctx.Done() != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (m *mysqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	s, err := m.Prepare(query)
	if err != nil {
		return nil, err
	}

	if ctx.Done() != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return s, err
}

func (m *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	m.curSeqID = 0
	err := m.prw.Write(&StmtPrepare{Name: 0x16, Query: query}, m.curSeqID)
	if err != nil {
		return nil, fmt.Errorf("prepare error: %v", err)
	}

	prepareResp, err := m.prw.readStmtPrepareResp()
	if err != nil {
		return nil, fmt.Errorf("prepare resp error: %v", err)
	}

	// 初始化 stmt
	return nil, nil
}

func (m *mysqlConn) Close() error {
	if m.closed {
		return nil
	}

	m.closed = true
	err := m.conn.Close()
	return err
}

func (m *mysqlConn) Begin() (driver.Tx, error) {
	return nil, nil
}


