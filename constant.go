package mysql

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
