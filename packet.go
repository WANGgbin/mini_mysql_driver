package mysql

type HandshakeV10 struct {
	ProcVer  uint8  `mpdt:"1"`
	SrvVer   string `mpdt:"nullTerminated"`
	ThreadID uint32 `mpdt:"4"`
	// 8 字节
	AuthDataPartOne string `mpdt:"fixed,8"`
	Pad             uint8  `mpdt:"1"`
	CapFlagsPartOne uint16 `mpdt:"2"`
	CharSet         uint8  `mpdt:"1"`
	SrvStatus       uint16 `mpdt:"2"`
	CapFlagsPartTwo uint16 `mpdt:"2"`
	AuthDataLen     uint8  `mpdt:"1" cap:"pluginAuth"`
	UnUse           uint8  `mpdt:"1" cap:"!pluginAuth"`
	// 10 字节
	Reserved        string `mpdt:"fixed,10"`
	AuthDataPartTwo string `mpdt:"fixed,13"`
	AuthPluginName  string `mpdt:"nullTerminated" cap:"pluginAuth"`
}

type HandshakeResp41 struct {
	CapFlag       CapFlag `mpdt:"4"` // mpdt means mysql protocol basic data type
	MaxPacketSize uint32  `mpdt:"4"`
	CharSet       uint8   `mpdt:"1"`
	Pad           string  `mpdt:"fixed"`
	UserName      string  `mpdt:"nullTerminated"`
	AuthRespLen   uint8   `mpdt:"1"`
	AuthResp      string  `mpdt:"fixed"`
	DataBase      string  `mpdt:"nullTerminated" cap:"connectWithDb"`
	PluginName    string  `mpdt:"nullTerminated" cap:"pluginAuth"`
}

type AuthSwitchReq struct {
	// 固定值 0xFE
	StatusFlag uint8  `mpdt:"1"`
	PluginName string `mpdt:"nullTerminated"`
	PluginData string `mpdt:"rest"`
}

type AuthSwitchResp struct {
	AuthData string `mpdt:"rest"`
}

type ErrPacket struct {
	Hdr     uint8  `mpdt:"1"`
	ErrCode uint16 `mpdt:"2"`
	// 1 字节
	SqlStateMarker string `mpdt:"fixed,1" cap:"protocol41"`
	// 5 字节
	SqlState string `mpdt:"fixed,5" cap:"protocol41"`
	ErrMsg   string `mpdt:"rest"`
}

type OkPacket struct {
	Hdr              uint8  `mpdt:"1"`
	AffectedRows     uint64 `mpdt:"varInt"`
	LastInsertId     uint64 `mpdt:"varInt"`
	SrvStatus        uint16 `mpdt:"2" cap:"protocol41||transactions"`
	Warnings         uint16 `mpdt:"2" cap:"protocol41"`
	SessionTrackInfo string `mpdt:"lenEncoded" cap:"sessionTrack"`
	SessionStateInfo string `mpdt:"lenEncoded" cap:"sessionTrack" srvStatus:"sessionStateChanged"`
	Info             string `mpdt:"rest" cap:"!sessionTrack"`
}

type SessionTrackInfoBlock struct {
	StateType uint8  `mpdt:"1"`
	Data      string `mpdt:"lenEncoded"`
}

type SessionTrackSysVar struct {
	Name  string `mpdt:"lenEncoded"`
	Value string `mpdt:"lenEncoded"`
}

type SessionTrackSchema struct {
	Name string `mpdt:"lenEncoded"`
}

/*
********** UTILITY COMMANDS **********
 */

// CmdPing 检查服务端是否存活, Name: 0x0E
type CmdPing struct {
	Name uint8 `mpdt:"1"`
}

/*
********** PREPARED STATEMENTS **********
 */

// StmtPrepare 创建预定义语句，Name: 0x16
type StmtPrepare struct {
	Name  uint8  `mpdt:"1"`
	Query string `mpdt:"rest"`
}

type StmtPrepareOK struct {
	// 固定值 0x00
	Status    uint8  `mpdt:"1"`
	StmtID    uint32 `mpdt:"4"`
	NumCols   uint16 `mpdt:"2"`
	NumParams uint16 `mpdt:"2"`
	Reserved  uint8  `mpdt:"1"`
}

type ColumnDef41 struct {
	// 默认值 "def"
	Catalog   string `mpdt:"lenEncoded"`
	Schema    string `mpdt:"lenEncoded"`
	Tbl       string `mpdt:"lenEncoded"`
	OrgTbl    string `mpdt:"lenEncoded"`
	Name      string `mpdt:"lenEncoded"`
	OrgName   string `mpdt:"lenEncoded"`
	Pad       uint64 `mpdt:"varInt"`
	CharSet   uint16 `mpdt:"2"`
	ColMaxLen uint32 `mpdt:"4"`

	// https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html#a69e798807026a0f7e12b1d6c72374854
	Type uint8 `mpdt:"1"`

	// https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
	Flags uint16 `mpdt:"2"`

	Decimals uint8 `mpdt:"1"`
}

type StmtExec struct {
}

type ColumnCount struct {
	ColCount uint64 `mpdt:"varInt"`
}

type BinaryResultSet struct {
	cnt  *ColumnCount
	cols []*ColumnDef41
}

type StmtClose struct {
	// 固定值 0x19
	Status uint8  `mpdt:"1"`
	StmtID uint32 `mpdt:"4"`
}

/*
********** TEXT PROTOCOL **********
 */

// CmdQuery 当 sql 没有参数的时候，就可以考虑直接使用 CmdQuery 命令，这会减少 prepare/close statement 这样的 roundtrip
// 从而提高性能
type CmdQuery struct {
	// 固定值 0x03
	Name uint8  `mpdt:"1"`
	SQL  string `mpdt:"rest"`
}
