package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"net"
	"time"
)

func init() {
	sql.Register("mini_mysql", &mysqlDriver{})
}

var _ driver.Driver = (*mysqlDriver)(nil)

type mysqlDriver struct{}

func (m *mysqlDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := parseDsn(dsn)
	if err != nil {
		return nil, fmt.Errorf("parseDsn error: %v", err)
	}

	return newMysqlConn(cfg)
}

var _ driver.Conn = (*mysqlConn)(nil)
var _ driver.ConnPrepareContext = (*mysqlConn)(nil)
var _ driver.Pinger = (*mysqlConn)(nil)
var _ driver.ConnBeginTx = (*mysqlConn)(nil)
var _ driver.Validator = (*mysqlConn)(nil)

type mysqlConn struct {
	dbCfg    *dbCfg
	conn     net.Conn
	prw      *pktReadWriter
	capFlag  CapFlag
	curSeqID uint8
	closed   bool
}

var defaultTimeOut = 1 * time.Second

// newMysqlConn 是否有必要池化 mysqlConn 对象
// 发生任何错误都应该返回 driver.ErrBadConn 这样 sql 包会进行重试
func newMysqlConn(db *dbCfg) (*mysqlConn, error) {
	dialer := &net.Dialer{
		// TODO(@wangguobin): 用户可配置建立连接超时时间
		Timeout:   defaultTimeOut,
		KeepAlive: 30 * time.Minute,
	}
	conn, err := dialer.Dial(db.protocol, fmt.Sprintf("%s:%d", db.ip, db.port))
	if err != nil {
		logrus.Errorf("when creating new conn, err happened: %v", err)
		return nil, driver.ErrBadConn
	}

	mc := &mysqlConn{
		dbCfg: db,
		conn:  conn,
	}

	mc.prw = newPktReadWriter(mc)

	err = mc.login()
	if err != nil {
		_ = mc.Close()
		logrus.Errorf("when login, err happened: %v", err)
		return nil, driver.ErrBadConn
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
		return m.initAfterLogin()
	}

	return errors.New("after sending switch auth resp, but receive switch auth req again")
}

// initAfterLogin 成功注册后，完成一些初始化，包括：时区等会话变量的设置
func (m *mysqlConn) initAfterLogin() error {
	return m.setTimeZone()
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

	err = m.prw.write(resp, m.curSeqID)
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
		CharSet:       m.dbCfg.charsetNum,
		Pad:           Byte2Str(make([]byte, 23)),
		UserName:      m.dbCfg.user,
		AuthRespLen:   uint8(len(authResp)),
		AuthResp:      Byte2Str(authResp),
		DataBase:      m.dbCfg.dbName,
		PluginName:    hs.AuthPluginName,
	}, nil
}

func (m *mysqlConn) sendAuthSwitchResp(req *AuthSwitchReq) error {
	resp, err := m.newAuthSwitchResp(req)
	if err != nil {
		return err
	}

	return m.prw.write(resp, m.curSeqID)
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
		return buildAuthRespWithCachingSha2Password(scramble, m.dbCfg.password), nil
	case "mysql_native_password":
		return buildAuthRespWithMysqlNativePassword(scramble[:20], m.dbCfg.password), nil
	default:
		return nil, fmt.Errorf("unknown authMethod: %s", authMethod)
	}
}

// Ping 发送 CmdPing 检测服务端是否存活
func (m *mysqlConn) Ping(ctx context.Context) error {
	m.curSeqID = 0
	err := m.prw.write(&CmdPing{Name: 0x0E}, m.curSeqID)
	if err != nil {
		return m.handleWriteError(err)
	}

	if _, err = m.prw.readOkPkt("Ping"); err != nil {
		_ = m.Close()
		return err
	}

	// 判断 ctx 是否取消
	if ctx.Done() != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
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
			_ = s.Close()
			return nil, ctx.Err()
		default:
		}
	}

	return s, nil
}

func (m *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	m.curSeqID = 0
	err := m.prw.write(&StmtPrepare{Name: 0x16, Query: query}, m.curSeqID)
	if err != nil {
		return nil, m.handleWriteError(err)
	}

	prepareResp, err := m.prw.readStmtPrepareResp()
	if err != nil {
		return nil, m.handleReadError(err)
	}

	// 初始化 stmt
	return m.buildStmt(prepareResp)
}

// buildStmt 在我们的实现中 CapClientDeprecatedEof 总是设置的，因此在 params 和 cols pkt 之后没有 EOF pkt
func (m *mysqlConn) buildStmt(resp *StmtPrepareOK) (*stmt, error) {
	var err error
	defer func() {
		if err != nil {
			_ = m.Close()
		}
	}()
	var params, cols []*ColumnDef41
	if resp.NumParams > 0 && !m.capFlag.IsSet(CapClientOptResultSetMetadata) {
		params = make([]*ColumnDef41, 0, int(resp.NumParams))
		for idx := 0; idx < int(resp.NumParams); idx++ {
			var param ColumnDef41
			err = m.prw.read(&param)
			if err != nil {
				return nil, err
			}
			params = append(params, &param)
		}
	}
	if resp.NumCols > 0 && !m.capFlag.IsSet(CapClientOptResultSetMetadata) {
		cols = make([]*ColumnDef41, 0, int(resp.NumCols))
		for idx := 0; idx < int(resp.NumCols); idx++ {
			var col ColumnDef41
			err = m.prw.read(&col)
			if err != nil {
				return nil, err
			}
			cols = append(cols, &col)
		}
	}

	return &stmt{
		okResp: resp,
		Params: params,
		Cols:   cols,
		mc:     m,
	}, nil
}

func (m *mysqlConn) Close() error {
	if m.closed {
		return nil
	}

	m.closed = true
	return m.conn.Close()
}

func (m *mysqlConn) isClosed() bool {
	return m.closed
}

// Begin 仅仅为了实现 driver.Conn。
// Deprecated
func (m *mysqlConn) Begin() (driver.Tx, error) {
	return nil, nil
}

func (m *mysqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// 如果不是默认隔离级别，开启事务前，需要先设置隔离级别
	if err := m.setIsolationLevel(sql.IsolationLevel(opts.Isolation)); err != nil {
		return nil, m.handleWriteError(err)
	}

	query := "START TRANSACTION"
	if opts.ReadOnly {
		query += " READ ONLY"
	}

	if err := m.prw.execCmdQuery(query); err != nil {
		_ = m.Close()
		return nil, m.handleWriteError(err)
	}

	return &tx{
		conn: m,
	}, nil
}

func (m *mysqlConn) setIsolationLevel(level sql.IsolationLevel) error {
	if level == sql.LevelDefault {
		return nil
	}

	if _, support := SupportedIsolationLevelSet[level]; !support {
		return fmt.Errorf("unsupported isolationLevel %s", level.String())
	}

	return m.prw.execCmdQuery(fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s", level.String()))
}

// IsValid sql 会调用 IsValid 来判断连接是否有效
func (m *mysqlConn) IsValid() bool {
	return !m.closed
}

func (m *mysqlConn) setTimeZone() error {
	_, offset := time.Now().Zone()
	// val 需要携带双引号
	val := fmt.Sprintf("\"%+03d:00\"", offset/3600)

	return m.setSessionVariable("time_zone", val)
}

func (m *mysqlConn) setSessionVariable(key, value string) error {
	return m.setVariable(key, value, false)
}

func (m *mysqlConn) setGlobalVariable(key, value string) error {
	return m.setVariable(key, value, true)
}

func (m *mysqlConn) setVariable(key, value string, isGlobal bool) error {
	var query string
	if isGlobal {
		query = fmt.Sprintf("SET GLOBAL %s = %s", key, value)
	} else {
		query = fmt.Sprintf("SET %s = %s", key, value)
	}

	m.curSeqID = 0
	return m.prw.execCmdQuery(query)
}

func (m *mysqlConn) handleReadError(err error) error {
	// 实际上，即使是 ReadErrTypeErrPkt，某些时候仍然需要关闭连接
	if readErr, ok := err.(*ErrorReadWritePkt); !ok || readErr.errType != ReadErrTypeErrPkt {
		_ = m.Close()
	}
	return err
}

// handleWriteError 是否需要重试
// tcp 连接可能是从连接池中拿到的旧连接，很有可能该链接已经关闭，因此我们需要区分这种类型的错误(writeZeroBytes)，
// 并在这种错误发生的时候，通过返回 driver.ErrBadConn 的方式通过 sql 包进行重试。
func (m *mysqlConn) handleWriteError(err error) error {
	_ = m.Close()
	if writeErr, ok := err.(*ErrorReadWritePkt); ok && writeErr.errType == WriteErrTypeWriteZeroBytes {
		return driver.ErrBadConn
	}

	return err
}

