package mysql

import "database/sql"

const (
	PacketTypeOK  = 0x00
	PacketTypeEOF = 0x00
	PacketTypeERR = 0x00
)

const (
	ColFlagNotNull     = 0
	ColFlagPriKey      = 1
	ColFlagUniqKey     = 2
	ColFlagMultiKey    = 3
	ColFlagUnsigned    = 5
	ColFlagAutoIncr    = 9
	ColFlagTimestamp   = 10
	ColFlagNoDflVal    = 12
	ColFlagNowOnUpdate = 13
	ColFlagGroup       = 15
)

const (
	ColTypeDecimal  byte = iota
	ColTypeTiny
	ColTypeShort
	ColTypeLong
	ColTypeFloat
	ColTypeDouble
	ColTypeNULL
	ColTypeTimestamp
	ColTypeLongLong
	ColTypeInt24
	ColTypeDate
	ColTypeTime
	ColTypeDateTime
	ColTypeYear
	ColTypeNewDate
	ColTypeVarChar
	ColTypeBit
)

const (
	ColTypeJSON byte = iota + 0xf5
	ColTypeNewDecimal
	ColTypeEnum
	ColTypeSet
	ColTypeTinyBLOB
	ColTypeMediumBLOB
	ColTypeLongBLOB
	ColTypeBLOB
	ColTypeVarString
	ColTypeString
	ColTypeGeometry
)

var SupportedIsolationLevelSet = map[sql.IsolationLevel]struct{} {
	sql.LevelReadUncommitted: {},
	sql.LevelReadCommitted: {},
	sql.LevelRepeatableRead: {},
	sql.LevelSerializable: {},
}

var charset2Num = map[string]uint8 {
	"utf8mb4": 45,
}