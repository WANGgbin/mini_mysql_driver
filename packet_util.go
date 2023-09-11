package mysql

import (
	"bufio"
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	PktTypeHandshake = iota
	PktTypeAuthSwitchReq
	PktTypeOk
	PktTypeErr
)

type Packet struct {
	length  uint32 `mpdt:"3"`
	seqID   uint8  `mpdt:"1"`
	payload []byte
}

func newPacket(seqID uint8, payload []byte) *Packet {
	return &Packet{
		length:  uint32(len(payload)),
		seqID:   seqID,
		payload: payload,
	}
}

func newPktReadWriter(conn *mysqlConn) *pktReadWriter {
	brw := bufio.NewReadWriter(bufio.NewReader(conn.conn), bufio.NewWriter(conn.conn))
	return &pktReadWriter{
		conn: conn,
		mrw: &mustReadWriter{
			brw: brw,
		},
	}
}

// pktReadWriter 数据包的收发器
type pktReadWriter struct {
	conn *mysqlConn
	mrw  *mustReadWriter
}

// execCmdQuery 执行 cmdQuery 命令
func (prw *pktReadWriter) execCmdQuery(query string) error {
	prw.conn.curSeqID = 0
	err := prw.write(
		&CmdQuery{
			Name: 0x03,
			SQL:  query,
		},
		prw.conn.curSeqID,
	)
	if err != nil {
		return err
	}

	if _, err = prw.readOkPkt(fmt.Sprintf("CmdQuery: %s", query)); err != nil {
		return fmt.Errorf("when exec query %s, got err from server: %v", query, err)
	}
	return nil
}

func (prw *pktReadWriter) read(pkt interface{}) error {
	Assert(reflect.TypeOf(pkt).Kind() == reflect.Ptr && reflect.TypeOf(pkt).Elem().Kind() == reflect.Struct, "pkt must be pointer to struct")
	pk, err := prw.doRead(reflect.TypeOf(pkt).Elem().Name())
	if err != nil {
		return err
	}
	err = extractPkt(&pk.payload, pkt, &prw.conn.capFlag)
	if err != nil {
		return &ErrorReadWritePkt{event: fmt.Sprintf("read pkt %s", reflect.TypeOf(pkt).Elem().Name()), errType: ReadErrTypeMalformedPkt, raw: err}
	}

	return nil
}

func (prw *pktReadWriter) readOkPkt(pktName string) (*OkPacket, error) {
	rawPkt, err := prw.doRead(pktName)
	if err != nil {
		return nil, err
	}

	var pkt interface{}
	switch rawPkt.payload[0] {
	case 0x00:
		pkt = new(OkPacket)
	case 0xFF:
		pkt = new(ErrPacket)
	default:
		return nil, &ErrorReadWritePkt{event: pktName, errType: ReadErrTypeUnknownPkt, raw: fmt.Errorf("staring with [%#X]", rawPkt.payload[0])}
	}

	err = extractPkt(&rawPkt.payload, pkt, &prw.conn.capFlag)
	if err != nil {
		return nil, &ErrorReadWritePkt{event: pktName, errType: ReadErrTypeMalformedPkt, raw: err}
	}

	if errPkt, ok := pkt.(*ErrPacket); ok {
		return nil, &ErrorReadWritePkt{event: pktName, errType: ReadErrTypeErrPkt, raw: fmt.Errorf("code: %d, msg: %s", errPkt.ErrCode, errPkt.ErrMsg)}
	}

	prw.conn.curSeqID = 0
	return pkt.(*OkPacket), nil
}

func (prw *pktReadWriter) readAuthResult() (interface{}, error) {
	rawPkt, err := prw.doRead("AuthResult")
	if err != nil {
		return nil, err
	}

	var pkt interface{}

	// Protocol::AuthMoreData:, ERR_Packet or OK_Packet
	switch rawPkt.payload[0] {
	case 0x00:
		pkt = new(OkPacket)
	case 0xFE:
		pkt = new(AuthSwitchReq)
	case 0xFF:
		pkt = new(ErrPacket)
	default:
		return nil, fmt.Errorf("receive unknown packet stating with [%#X] after sending auth resp", rawPkt.payload[0])
	}

	err = extractPkt(&rawPkt.payload, pkt, &prw.conn.capFlag)
	if err != nil {
		return nil, fmt.Errorf("extrack auth result pkt error: %v", err)
	}

	if errPkt, ok := pkt.(*ErrPacket); ok {
		return nil, fmt.Errorf("when authenticating, recieve err pkt from server: code: %d, msg: %s", errPkt.ErrCode, errPkt.ErrMsg)
	}

	return pkt, nil
}

func (prw *pktReadWriter) readStmtPrepareResp() (*StmtPrepareOK, error) {
	rawPkt, err := prw.doRead("StmtPrepareResp")
	if err != nil {
		return nil, err
	}

	var pkt interface{}
	// COM_STMT_PREPARE_OK on success, ERR_Packet otherwise
	switch rawPkt.payload[0] {
	case 0x00:
		pkt = new(StmtPrepareOK)
	case 0xFF:
		pkt = new(ErrPacket)
	default:
		return nil, &ErrorReadWritePkt{event: "read StmtPrepareResp", errType: ReadErrTypeUnknownPkt, raw: fmt.Errorf("stating with [%#X]", rawPkt.payload[0])}
	}

	err = extractPkt(&rawPkt.payload, pkt, &prw.conn.capFlag)
	if err != nil {
		return nil, &ErrorReadWritePkt{event: "read StmtPrepareResp", errType: ReadErrTypeMalformedPkt, raw: err}
	}

	if errPkt, ok := pkt.(*ErrPacket); ok {
		return nil, &ErrorReadWritePkt{event: "read StmtPrepareResp", errType: ReadErrTypeErrPkt, raw: fmt.Errorf("code: %d, msg: %s", errPkt.ErrCode, errPkt.ErrMsg)}
	}

	return pkt.(*StmtPrepareOK), nil
}

func (prw *pktReadWriter) readQueryResp() (*BinaryResultSet, error) {
	colCnt, err := prw.readColumnCount()
	if err != nil {
		return nil, err
	}

	cols := make([]*ColumnDef41, colCnt.ColCount)
	for idx := 0; idx < int(colCnt.ColCount); idx++ {
		curCol := new(ColumnDef41)
		err = prw.read(curCol)
		if err != nil {
			return nil, err
		}
		cols[idx] = curCol
	}

	return &BinaryResultSet{
		cnt:  colCnt,
		cols: cols,
	}, nil
}

func (prw *pktReadWriter) readColumnCount() (*ColumnCount, error) {
	pk, err := prw.doRead("ColumnCount")
	if err != nil {
		return nil, err
	}

	var pkt interface{}
	switch pk.payload[0] {
	case 0xFF:
		pkt = new(ErrPacket)
	default:
		pkt = new(ColumnCount)
	}

	err = extractPkt(&pk.payload, pkt, &prw.conn.capFlag)
	if err != nil {
		return nil, &ErrorReadWritePkt{event: "read ColumnCount", errType: ReadErrTypeMalformedPkt, raw: err}
	}

	if pkt, ok := pkt.(*ErrPacket); ok {
		return nil, &ErrorReadWritePkt{event: "read ColumnCount", errType: ReadErrTypeErrPkt, raw: fmt.Errorf("code: %d, msg: %s", pkt.ErrCode, pkt.ErrMsg)}
	}

	return pkt.(*ColumnCount), nil
}

// readNextRow 时间类型，默认转化为 time.Time 类型
func (prw *pktReadWriter) readNextRow(ret []driver.Value, cols []*ColumnDef41) error {
	Assert(len(ret) == len(cols), "len(driver.Value) != len(ColDef)")
	pk, err := prw.doRead("Row")
	if err != nil {
		return err
	}

	// 如果是 OK_Packet，需要返回 io.EOF
	if pk.payload[0] == 0xFE {
		return io.EOF
	}

	pk.payload = pk.payload[1:]
	nullBitMapLen := (len(cols) + 7 + 2) >> 3
	nullBitMapByte, err := extractFixedLengthByte(&pk.payload, nullBitMapLen)
	if err != nil {
		return fmt.Errorf("extract null bitmap error: %v", err)
	}
	nullBitMap := Bitmap(nullBitMapByte)

	for idx := 0; idx < len(cols); idx++ {
		if nullBitMap.IsSet(idx + 2) {
			ret[idx] = nil
			continue
		}
		// 各类型的编码方式可以参考：https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row
		// 不支持类型，后续补充
		switch cols[idx].Type {
		case ColTypeString, ColTypeVarString, ColTypeVarChar, ColTypeBLOB, ColTypeMediumBLOB, ColTypeTinyBLOB, ColTypeLongBLOB:
			ret[idx], err = extractMysqlTypeString(&pk.payload)
		case ColTypeDouble:
			ret[idx], err = extractMysqlTypeDouble(&pk.payload)
		case ColTypeFloat:
			ret[idx], err = extractMysqlTypeFloat(&pk.payload)
		case ColTypeDate:
			ret[idx], err = extractMysqlTypeDate(&pk.payload)
		case ColTypeDateTime:
			ret[idx], err = extractMysqlTypeDatetime(&pk.payload)
		case ColTypeTimestamp:
			ret[idx], err = extractMysqlTypeTimestamp(&pk.payload)
		case ColTypeLongLong:
			var val uint64
			val, err = extractMysqlTypeUnsignedLongLong(&pk.payload)
			colFlag := Bitmap(marshalUint16(cols[idx].Flags))
			if !colFlag.IsSet(ColFlagUnsigned) {
				ret[idx] = int64(val)
			} else {
				ret[idx] = val
			}
		case ColTypeLong:
			var val uint32
			val, err = extractMysqlTypeUnsignedLong(&pk.payload)
			colFlag := Bitmap(marshalUint16(cols[idx].Flags))
			if !colFlag.IsSet(ColFlagUnsigned) {
				ret[idx] = int32(val)
			} else {
				ret[idx] = val
			}
		case ColTypeShort:
			var val uint16
			val, err = extractMysqlTypeUnsignedShort(&pk.payload)
			colFlag := Bitmap(marshalUint16(cols[idx].Flags))
			if !colFlag.IsSet(ColFlagUnsigned) {
				ret[idx] = int16(val)
			} else {
				ret[idx] = val
			}
		case ColTypeTiny:
			var val uint8
			val, err = extractMysqlTypeUnsignedTiny(&pk.payload)
			colFlag := Bitmap(marshalUint16(cols[idx].Flags))
			if !colFlag.IsSet(ColFlagUnsigned) {
				ret[idx] = int8(val)
			} else {
				ret[idx] = val
			}
		default:
			return fmt.Errorf("unknown col type: %d", cols[idx].Type)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (prw *pktReadWriter) readVarInt() (uint64, error) {
	var first byte
	err := prw.mrw.read([]byte{first})
	if err != nil {
		return 0, err
	}

	if first < 0xFC {
		return uint64(first), nil
	}

	var length []byte
	switch first {
	case 0xFC:
		length = make([]byte, 2)
		err = prw.mrw.read(length)
		if err != nil {
			return 0, err
		}
		return extractInt(&length, 2)
	case 0xFD:
		length = make([]byte, 3)
		err = prw.mrw.read(length)
		if err != nil {
			return 0, err
		}
		return extractInt(&length, 3)
	case 0xFE:
		length = make([]byte, 8)
		err = prw.mrw.read(length)
		if err != nil {
			return 0, err
		}
		return extractInt(&length, 8)
	default:
		return 0, fmt.Errorf("unknown prefix of var int: %#X", first)
	}
}

func (prw *pktReadWriter) doRead(pktName string) (pkt *Packet, err error) {
	pkt = new(Packet)

	lenRaw := make([]byte, 3)
	err = prw.mrw.read(lenRaw)
	if err != nil {
		return nil, &ErrorReadWritePkt{event: fmt.Sprintf("read length of pkt %s", pktName), errType: ReadErrTypeSocket, raw: err}
	}

	length, err := extractInt(&lenRaw, 3)
	if err != nil {
		return nil, &ErrorReadWritePkt{event: fmt.Sprintf("read length of pkt: %s", pktName), errType: ReadErrTypeMalformedPkt, raw: err}
	}
	pkt.length = uint32(length)

	seqIDRaw := make([]byte, 1)
	err = prw.mrw.read(seqIDRaw)
	if err != nil {
		return nil, &ErrorReadWritePkt{event: fmt.Sprintf("read seqID of pkt: %s", pktName), errType: ReadErrTypeSocket, raw: err}
	}

	seqID, err := extractInt(&seqIDRaw, 1)
	if err != nil {
		return nil, &ErrorReadWritePkt{event: fmt.Sprintf("read seqID of pkt: %s", pktName), errType: ReadErrTypeMalformedPkt, raw: err}
	}
	pkt.seqID = uint8(seqID)

	data := make([]byte, pkt.length)
	err = prw.mrw.read(data)
	if err != nil {
		return nil, &ErrorReadWritePkt{event: fmt.Sprintf("read payload of pkt: %s", pktName), errType: ReadErrTypeSocket, raw: err}
	}
	pkt.payload = data

	// 调整数据包的序号
	prw.conn.curSeqID = pkt.seqID + 1
	return pkt, nil
}

func (prw *pktReadWriter) write(pkt interface{}, seqID uint8) error {
	buf := bytes.Buffer{}
	payload := marshalPkt(pkt, &prw.conn.capFlag)
	buf.Write(marshalInt(uint64(len(payload)), 3))
	buf.Write(marshalInt(uint64(seqID), 1))
	buf.Write(payload)

	data := buf.Bytes()

	err := prw.mrw.write(data)
	if err != nil {
		err.event = fmt.Sprintf("write pkt: %s", reflect.TypeOf(pkt).Elem().Name())
		return err
	}
	return nil
}

//  StmtExecPkt 格式参考：https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
func (prw *pktReadWriter) writeStmtExecPkt(stmtOk *StmtPrepareOK, args []driver.Value) error {
	buf := bytes.Buffer{}
	buf.WriteByte(0x17)
	buf.Write(marshalUint32(stmtOk.StmtID))
	buf.Write(marshalUint8(0))
	buf.Write(marshalUint32(1))
	if stmtOk.NumParams > 0 {
		nullBitMap := make([]byte, (stmtOk.NumParams+7)>>3)
		for idx, arg := range args {
			if arg == nil {
				nullBitMap[idx>>3] |= 1 << (idx & 0x07)
			}
		}
		buf.Write(nullBitMap)
		// 并不知道该参数的含义
		buf.WriteByte(0x01)

		paramTypeData := bytes.Buffer{}
		paramValData := bytes.Buffer{}
		for _, arg := range args {
			td, data, err := marshalDriverValue(arg)
			if err != nil {
				return &ErrorReadWritePkt{event: "write StmtExec", errType: WriteErrTypeMarshalError, raw: err}
			}
			paramTypeData.Write(td)
			paramValData.Write(data)
		}
		buf.Write(paramTypeData.Bytes())
		buf.Write(paramValData.Bytes())
	}

	prw.conn.curSeqID = 0
	return prw.writeBytes(buf.Bytes(), prw.conn.curSeqID)
}

// marshalDriverValue 获取 Driver.Value 的类型、值序列化，
// 在进入该函数之前，sql 会对用户的入参进行类型校验。
func marshalDriverValue(value driver.Value) ([]byte, []byte, error) {
	if value == nil {
		return []byte{ColTypeNULL, 0x00}, nil, nil
	}
	tb := make([]byte, 2)
	var data []byte
	switch v := value.(type) {
	case bool:
		tb[0] = ColTypeTiny
		tb[1] = 0x00
		if v {
			data = marshalInt8(0x01)
		} else {
			data = marshalInt8(0x00)
		}
	case uint64:
		tb[0] = ColTypeLongLong
		tb[1] = 0x80
		data = marshalUint64(v)
	case int64:
		tb[0] = ColTypeLongLong
		tb[1] = 0x00
		data = marshalInt64(v)
	case float64:
		tb[0] = ColTypeDouble
		tb[1] = 0x00
		data = marshalFloat64(v)
	case []byte:
		tb[0] = ColTypeString
		tb[1] = 0x00
		data = marshalLengthEncodeString(Byte2Str(v))
	case string:
		tb[0] = ColTypeString
		tb[1] = 0x00
		data = marshalLengthEncodeString(v)
	case time.Time:
		// 时间类型以字符串格式序列化
		tb[0] = ColTypeString
		tb[1] = 0x00
		data = marshalTime(v)
	default:
		return tb, nil, errors.New("type of Driver.Value should be one of [u]int64/float64/bool/[]byte/string/time.Time")
	}
	return tb, data, nil
}

func (prw *pktReadWriter) writeBytes(payload []byte, seqID uint8) error {
	buf := bytes.Buffer{}
	buf.Write(marshalInt(uint64(len(payload)), 3))
	buf.Write(marshalInt(uint64(seqID), 1))
	buf.Write(payload)

	data := buf.Bytes()

	err := prw.mrw.write(data)
	if err != nil {
		return err
	}
	return nil
}

type mustReadWriter struct {
	brw *bufio.ReadWriter
}

// read 一定能够读取 len(p) 大小的字节流，否则报错
func (m *mustReadWriter) read(p []byte) error {
	for idx := 0; idx < len(p); {
		n, err := m.brw.Read(p[idx:])
		if err != nil {
			return err
		}
		idx += n
	}

	return nil
}

func (m *mustReadWriter) write(p []byte) *ErrorReadWritePkt {
	Assert(len(p) > 0, "length of data to write must be greater than 0")
	n, err := m.brw.Write(p)
	if err != nil {
		if n == 0 {
			return &ErrorReadWritePkt{event: "write data", errType: WriteErrTypeWriteZeroBytes, raw: err}
		}
		return &ErrorReadWritePkt{event: "write data", errType: WriteErrTypeWriteSocket, raw: err}
	}

	err = m.brw.Flush()
	if err != nil {
		return &ErrorReadWritePkt{event: "flush data", errType: WriteErrTypeWriteSocket, raw: err}
	}

	return nil
}

// marshalPkt 序列化数据包。会根据 flag 选择性的序列化某些字段
func marshalPkt(pkt interface{}, cf *CapFlag) []byte {
	val := reflect.ValueOf(pkt)
	Assert(val.Kind() == reflect.Ptr && val.Elem().Kind() == reflect.Struct, "pkt must be a ptr to struct")

	buf := bytes.Buffer{}
	elemTyp := val.Elem().Type()
	elemVal := val.Elem()
	for idx := 0; idx < elemTyp.NumField(); idx++ {
		fieldTyp := elemTyp.Field(idx)
		fieldVal := elemVal.Field(idx)

		tag := fieldTyp.Tag.Get("mpdt")
		if tag == "" {
			continue
		}

		// 在 flag 满足 condTag 条件下才序列化该字段
		c := fieldTyp.Tag.Get("cap")
		if !satisfyCap(*cf, c) {
			continue
		}
		tagVals := strings.Split(tag, ",")
		switch tagVals[0] {
		case "1":
			data := fieldVal.Uint()
			buf.Write(marshalInt(data, 1))
		case "2":
			data := fieldVal.Uint()
			buf.Write(marshalInt(data, 2))
		case "3":
			data := fieldVal.Uint()
			buf.Write(marshalInt(data, 3))
		case "4":
			data := fieldVal.Uint()
			buf.Write(marshalInt(data, 4))
		case "6":
			data := fieldVal.Uint()
			buf.Write(marshalInt(data, 6))
		case "8":
			data := fieldVal.Uint()
			buf.Write(marshalInt(data, 8))
		case "varInt":
			data := fieldVal.Uint()
			buf.Write(marshalVarInt(data))
		case "fixed", "rest":
			buf.WriteString(fieldVal.String())
		case "nullTerminated":
			buf.WriteString(fieldVal.String())
			buf.WriteByte(0x00)
		case "lenEncoded":
			data := fieldVal.String()
			buf.Write(marshalLengthEncodeString(data))
		default:
			panic(fmt.Sprintf("unknown val of mpdt tag: %s", tagVals[0]))
		}
	}

	return buf.Bytes()
}

func extractPkt(dataPtr *[]byte, pkt interface{}, cf *CapFlag) error {
	val := reflect.ValueOf(pkt)
	Assert(val.Kind() == reflect.Ptr && val.Elem().Kind() == reflect.Struct, "pkt must be a pointer to struct")

	elem := val.Elem()
	typ := val.Elem().Type()
	var ss *srvStatus
	var capFlagOne, capFlagTwo uint16

	for idx := 0; idx < typ.NumField(); idx++ {
		fieldTyp := typ.Field(idx)
		fieldVal := elem.Field(idx)

		mpdtTag := fieldTyp.Tag.Get("mpdt")
		if mpdtTag == "" {
			continue
		}

		c := fieldTyp.Tag.Get("cap")
		status := fieldTyp.Tag.Get("srvStatus")
		if !satisfyCap(*cf, c) || !satisfySrvStatus(ss, status) {
			continue
		}

		mpdtTagVals := strings.Split(mpdtTag, ",")
		switch mpdtTagVals[0] {
		case "1":
			data, err := extractInt(dataPtr, 1)
			if err != nil {
				return err
			}
			fieldVal.SetUint(data)
		case "2":
			data, err := extractInt(dataPtr, 2)
			if err != nil {
				return err
			}
			fieldVal.SetUint(data)
			if fieldTyp.Name == "SrvStatus" {
				tmp := srvStatus(data)
				ss = &tmp
			} else if fieldTyp.Name == "CapFlagsPartOne" {
				capFlagOne = uint16(data)
			} else if fieldTyp.Name == "CapFlagsPartTwo" {
				capFlagTwo = uint16(data)
				*cf = combCapFlag(capFlagOne, capFlagTwo)
			}
		case "3":
			data, err := extractInt(dataPtr, 3)
			if err != nil {
				return err
			}
			fieldVal.SetUint(data)
		case "4":
			data, err := extractInt(dataPtr, 4)
			if err != nil {
				return err
			}
			fieldVal.SetUint(data)
		case "6":
			data, err := extractInt(dataPtr, 6)
			if err != nil {
				return err
			}
			fieldVal.SetUint(data)
		case "8":
			data, err := extractInt(dataPtr, 8)
			if err != nil {
				return err
			}
			fieldVal.SetUint(data)
		case "varInt":
			data, err := extractVarInt(dataPtr)
			if err != nil {
				return err
			}
			fieldVal.SetUint(data)
		case "fixed":
			Assert(len(mpdtTagVals) >= 2, "must specify length of fixed string")
			length, err := strconv.ParseUint(mpdtTagVals[1], 10, 64)
			Assert(err == nil, "length of fixed string must be uint")
			data, err := extractFixedLengthString(dataPtr, int(length))
			if err != nil {
				return err
			}
			fieldVal.SetString(data)
		case "nullTerminated":
			data, err := extractNullTerminatedString(dataPtr)
			if err != nil {
				return err
			}
			fieldVal.SetString(data)
		case "lenEncoded":
			data, err := extractLengthEncodeString(dataPtr)
			if err != nil {
				return err
			}
			fieldVal.SetString(data)
		case "rest":
			data, err := extractRestOfPacketString(dataPtr)
			if err != nil {
				return err
			}
			fieldVal.SetString(data)
		default:
			panic(fmt.Sprintf("unknown val of mpdt tag: %s", mpdtTagVals[0]))
		}
	}

	return nil
}

func satisfyCap(flag CapFlag, cond string) bool {
	if cond == "" {
		return true
	}
	Assert(flag != 0, "capFlag is empty")

	caps := strings.Split(cond, "||")
	for _, c := range caps {
		unset := false
		if strings.HasPrefix(cond, "!") {
			unset = true
			c = c[1:]
		}
		pos, exist := name2CapFlag[c]
		if !exist {
			panic(fmt.Sprintf("unknown flag: %s", cond))
		}

		if unset && !flag.IsSet(pos) {
			return true
		}

		if !unset && flag.IsSet(pos) {
			return true
		}
	}

	return false
}

func satisfySrvStatus(status *srvStatus, cond string) bool {
	if cond == "" {
		return true
	}

	Assert(status != nil, "srvStatus is empty")

	caps := strings.Split(cond, "||")
	for _, c := range caps {
		unset := false
		if strings.HasPrefix(cond, "!") {
			unset = true
			c = c[1:]
		}
		pos, exist := name2SrvStatus[c]
		if !exist {
			panic(fmt.Sprintf("unknown status: %s", cond))
		}

		if unset && !status.isSet(pos) {
			return true
		}

		if !unset && status.isSet(pos) {
			return true
		}
	}

	return false
}

func marshalInt8(val int8) []byte {
	return []byte{byte(val)}
}

func marshalUint8(val uint8) []byte {
	return []byte{byte(val)}
}

func marshalInt16(val int16) []byte {
	ret := make([]byte, 2)
	ret[0] = byte(val)
	ret[1] = byte(val >> 8)

	return ret
}

func marshalUint16(val uint16) []byte {
	ret := make([]byte, 2)
	ret[0] = byte(val)
	ret[1] = byte(val >> 8)

	return ret
}

func marshalInt24(val int32) []byte {
	ret := make([]byte, 3)
	ret[0] = byte(val)
	ret[1] = byte(val >> 8)
	ret[2] = byte(val >> 16)

	return ret
}

func marshalUint24(val uint32) []byte {
	ret := make([]byte, 3)
	ret[0] = byte(val)
	ret[1] = byte(val >> 8)
	ret[2] = byte(val >> 16)

	return ret
}

func marshalInt32(val int32) []byte {
	ret := make([]byte, 4)
	ret[0] = byte(val)
	ret[1] = byte(val >> 8)
	ret[2] = byte(val >> 16)
	ret[3] = byte(val >> 24)

	return ret
}

func marshalUint32(val uint32) []byte {
	ret := make([]byte, 4)
	ret[0] = byte(val)
	ret[1] = byte(val >> 8)
	ret[2] = byte(val >> 16)
	ret[3] = byte(val >> 24)

	return ret
}

func marshalInt64(val int64) []byte {
	ret := make([]byte, 8)
	ret[0] = byte(val)
	ret[1] = byte(val >> 8)
	ret[2] = byte(val >> 16)
	ret[3] = byte(val >> 24)
	ret[4] = byte(val >> 32)
	ret[5] = byte(val >> 40)
	ret[6] = byte(val >> 48)
	ret[7] = byte(val >> 56)

	return ret
}

func marshalUint64(val uint64) []byte {
	ret := make([]byte, 8)
	ret[0] = byte(val)
	ret[1] = byte(val >> 8)
	ret[2] = byte(val >> 16)
	ret[3] = byte(val >> 24)
	ret[4] = byte(val >> 32)
	ret[5] = byte(val >> 40)
	ret[6] = byte(val >> 48)
	ret[7] = byte(val >> 56)

	return ret
}

func marshalInt(v uint64, length int) []byte {
	ret := make([]byte, length)

	for idx := 0; idx < length; idx++ {
		ret[idx] = byte(v >> (8 * idx))
	}

	return ret
}

func marshalVarInt(v uint64) []byte {
	if v < 251 {
		return marshalInt(v, 1)
	}

	if v < 2^16 {
		return append([]byte{0xFC}, marshalInt(v, 2)...)
	}

	if v < 2^24 {
		return append([]byte{0xFD}, marshalInt(v, 3)...)
	}

	return append([]byte{0xFE}, marshalInt(v, 8)...)
}

func marshalFloat32(f float32) []byte {
	return marshalUint32(math.Float32bits(f))
}

func marshalFloat64(f float64) []byte {
	return marshalUint64(math.Float64bits(f))
}

func marshalLengthEncodeString(data string) []byte {
	buf := bytes.Buffer{}
	buf.Write(marshalVarInt(uint64(len(data))))
	buf.WriteString(data)

	return buf.Bytes()
}

// marshalTime 序列化时间类型为 "yyyy-MM-ddThh:mm:ss.999999999" 格式
func marshalTime(t time.Time) []byte {
	year, month, day := t.Date()
	hour, minute, second := t.Clock()
	nanoSecond := t.Nanosecond()

	buf := bytes.Buffer{}
	buf.Grow(len("yyyy-MM-ddThh:mm:ss.999999999"))
	// 时间格式必须要指定 年、月、日
	buf.WriteString(fmt.Sprintf("%04d-%02d-%02d", year, month, day))

	// 时、分、秒、纳秒，如果都为0，可以不指定，直接返回
	if hour == 0 && minute == 0 && second == 0 && nanoSecond == 0 {
		return buf.Bytes()
	}

	buf.WriteByte('T')
	buf.WriteString(fmt.Sprintf("%02d:%02d:%02d", hour, minute, second))

	if nanoSecond == 0 {
		return buf.Bytes()
	}

	buf.WriteByte('.')
	buf.WriteString(fmt.Sprintf("%09d", nanoSecond))

	return marshalLengthEncodeString(Byte2Str(buf.Bytes()))
}

var (
	ErrLessLength = errors.New("less length")
)

func extractInt8(dataPtr *[]byte) (int8, error) {
	ret, err := extractInt(dataPtr, 1)
	if err != nil {
		return 0, err
	}
	return int8(ret), nil
}

func extractUint8(dataPtr *[]byte) (uint8, error) {
	ret, err := extractInt(dataPtr, 1)
	if err != nil {
		return 0, err
	}
	return uint8(ret), nil
}

func extractInt16(dataPtr *[]byte) (int16, error) {
	ret, err := extractInt(dataPtr, 2)
	if err != nil {
		return 0, err
	}
	return int16(ret), nil
}

func extractUint16(dataPtr *[]byte) (uint16, error) {
	ret, err := extractInt(dataPtr, 2)
	if err != nil {
		return 0, err
	}
	return uint16(ret), nil
}

func extractInt24(dataPtr *[]byte) (int32, error) {
	ret, err := extractInt(dataPtr, 3)
	if err != nil {
		return 0, err
	}
	return int32(ret), nil
}

func extractUint24(dataPtr *[]byte) (uint32, error) {
	ret, err := extractInt(dataPtr, 3)
	if err != nil {
		return 0, err
	}
	return uint32(ret), nil
}
func extractInt32(dataPtr *[]byte) (int32, error) {
	ret, err := extractInt(dataPtr, 4)
	if err != nil {
		return 0, err
	}
	return int32(ret), nil
}

func extractUint32(dataPtr *[]byte) (uint32, error) {
	ret, err := extractInt(dataPtr, 4)
	if err != nil {
		return 0, err
	}
	return uint32(ret), nil
}
func extractInt64(dataPtr *[]byte) (int64, error) {
	ret, err := extractInt(dataPtr, 8)
	if err != nil {
		return 0, err
	}
	return int64(ret), nil
}

func extractUint64(dataPtr *[]byte) (uint64, error) {
	ret, err := extractInt(dataPtr, 8)
	if err != nil {
		return 0, err
	}
	return ret, nil
}

func extractInt(dataPtr *[]byte, length int) (uint64, error) {
	data := *dataPtr
	if len(data) < length {
		return 0, ErrLessLength
	}

	var ret uint64
	for idx := 0; idx < length; idx++ {
		ret |= uint64(data[idx]) << (8 * idx)
	}

	*dataPtr = (*dataPtr)[length:]
	return ret, nil
}

func extractVarInt(dataPtr *[]byte) (uint64, error) {
	data := *dataPtr

	fb := uint64(data[0])

	if fb < 0xFC {
		*dataPtr = (*dataPtr)[1:]
		return fb, nil
	}

	last := data[1:]
	var val uint64
	var err error

	switch fb {
	case 0xFC:
		val, err = extractInt(&last, 2)
	case 0xFD:
		val, err = extractInt(&last, 3)
	case 0xFE:
		val, err = extractInt(&last, 8)
	default:
		return 0, fmt.Errorf("unknown prefix of var int: %#X", fb)
	}

	if err != nil {
		return 0, nil
	}
	*dataPtr = last
	return val, nil
}

func extractFixedLengthString(data *[]byte, length int) (string, error) {
	if len(*data) < length {
		return "", ErrLessLength
	}

	ret := string((*data)[:length])
	*data = (*data)[length:]
	return ret, nil
}

func extractFixedLengthByte(data *[]byte, length int) ([]byte, error) {
	if len(*data) < length {
		return nil, ErrLessLength
	}

	ret := (*data)[:length:length]
	*data = (*data)[length:]
	return ret, nil
}

func extractNullTerminatedString(dataPtr *[]byte) (string, error) {
	idx := 0
	data := *dataPtr
	for ; idx < len(data); idx++ {
		if (data)[idx] == 0x00 {
			break
		}
	}

	if idx == len(data) {
		return "", errors.New("no 0x00 in data")
	}

	ret := string((data)[:idx])
	*dataPtr = (data)[idx+1:]

	return ret, nil
}

// extractLengthEncodeString 变长字符串格式：length | data 、 0x00(后面还有其他字段) 、 空(后面无其他字段)
func extractLengthEncodeString(dataPtr *[]byte) (string, error) {
	if len(*dataPtr) == 0 {
		return "", nil
	}
	length, err := extractVarInt(dataPtr)
	if err != nil {
		return "", err
	}

	if uint64(len(*dataPtr)) < length {
		return "", ErrLessLength
	}

	ret := Byte2Str((*dataPtr)[:length])
	*dataPtr = (*dataPtr)[length:]
	return ret, nil
}

func extractLengthEncodeByte(dataPtr *[]byte) ([]byte, error) {
	if len(*dataPtr) == 0 {
		return nil, nil
	}
	length, err := extractVarInt(dataPtr)
	if err != nil {
		return nil, err
	}

	if uint64(len(*dataPtr)) < length {
		return nil, ErrLessLength
	}

	ret := (*dataPtr)[:length:length]
	*dataPtr = (*dataPtr)[length:]
	return ret, nil

}

func extractRestOfPacketString(dataPtr *[]byte) (string, error) {
	data := *dataPtr
	if len(data) == 0 {
		return "", ErrLessLength
	}
	ret := data[0:]
	*dataPtr = data[:len(data)]
	return string(ret), nil
}

/*

********** MARSHAL/EXTRACT MYSQL TYPE **********
关于 mysql 各个类型序列化方式可以参考：https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row

*/

func extractMysqlTypeString(dataPtr *[]byte) (string, error) {
	return extractLengthEncodeString(dataPtr)
}

func extractMysqlTypeUnsignedLongLong(dataPtr *[]byte) (uint64, error) {
	return extractUint64(dataPtr)
}

func extractMysqlTypeUnsignedLong(dataPtr *[]byte) (uint32, error) {
	return extractUint32(dataPtr)
}

func extractMysqlTypeUnsignedInt24(dataPtr *[]byte) (uint32, error) {
	return extractUint24(dataPtr)
}

func extractMysqlTypeUnsignedShort(dataPtr *[]byte) (uint16, error) {
	return extractUint16(dataPtr)
}

func extractMysqlTypeUnsignedTiny(dataPtr *[]byte) (uint8, error) {
	return extractUint8(dataPtr)
}

func extractMysqlTypeDouble(dataPtr *[]byte) (float64, error) {
	ret, err := extractInt(dataPtr, 8)
	if err != nil {
		return 0, err
	}

	return math.Float64frombits(ret), nil
}

func extractMysqlTypeFloat(dataPtr *[]byte) (float32, error) {
	ret, err := extractInt(dataPtr, 4)
	if err != nil {
		return 0, err
	}

	return math.Float32frombits(uint32(ret)), nil
}

func extractMysqlTypeDate(dataPtr *[]byte) (time.Time, error) {
	return doExtractMysqlTypeDate(dataPtr)
}

func extractMysqlTypeDatetime(dataPtr *[]byte) (time.Time, error) {
	return doExtractMysqlTypeDate(dataPtr)
}

func extractMysqlTypeTimestamp(dataPtr *[]byte) (time.Time, error) {
	return doExtractMysqlTypeDate(dataPtr)
}

func doExtractMysqlTypeDate(dataPtr *[]byte) (time.Time, error) {
	length, err := extractUint8(dataPtr)
	if err != nil {
		return time.Time{}, err
	}

	if length == 0 {
		return time.Time{}, nil
	}

	year, err := extractUint16(dataPtr)
	if err != nil {
		return time.Time{}, err
	}

	month, err := extractUint8(dataPtr)
	if err != nil {
		return time.Time{}, err
	}

	day, err := extractUint8(dataPtr)
	if err != nil {
		return time.Time{}, err
	}

	if length == 4 {
		// TODO: 握手阶段，需要指定会话时区信息 parseTime=true&loc=Local
		t := time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.Local)
		return t, nil
	}

	hour, err := extractUint8(dataPtr)
	if err != nil {
		return time.Time{}, err
	}

	minute, err := extractUint8(dataPtr)
	if err != nil {
		return time.Time{}, err
	}

	second, err := extractUint8(dataPtr)
	if err != nil {
		return time.Time{}, err
	}

	if length == 7 {
		t := time.Date(int(year), time.Month(month), int(day), int(hour), int(minute), int(second), 0, time.Local)
		return t, nil
	}

	microSecond, err := extractUint32(dataPtr)
	if err != nil {
		return time.Time{}, err
	}

	t := time.Date(int(year), time.Month(month), int(day), int(hour), int(minute), int(second), int(microSecond*1000), time.Local)
	return t, nil
}

// 参考：https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html
const (
	CapClientLongPassword         = 0
	CapClientColumnLongFlag       = 2
	CapClientProtocol41           = 9
	CapClientConnectWithDB        = 3
	CapClientPluginAuth           = 19
	CapClientTransactions         = 13
	CapClientSessionTrack         = 23
	CapClientMultiResults         = 16
	CapClientDeprecateEof         = 24
	CapClientAuthentication41     = 15
	CapClientOptResultSetMetadata = 25
	CapClientOptQueryAttributes   = 27
)

var name2CapFlag = map[string]int{
	"protocol41":    CapClientProtocol41,
	"connectWithDb": CapClientConnectWithDB,
	"pluginAuth":    CapClientPluginAuth,
	"transactions":  CapClientTransactions,
	"sessionTrack":  CapClientSessionTrack,
}

func combCapFlag(partOne, partTwo uint16) CapFlag {
	ret := CapFlag(partOne)
	ret |= CapFlag(partTwo) << 16
	return ret
}

func buildClientCapFlag() (cf CapFlag) {
	cpbs := []int{
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
	}

	return newCapFlag(cpbs...)
}

func newCapFlag(cpbs ...int) (cf CapFlag) {
	for _, cpb := range cpbs {
		cf.Set(cpb)
	}
	return
}

type CapFlag uint32

func (c *CapFlag) Set(pos int) {
	*c |= 1 << pos
}

func (c *CapFlag) UnSet(pos int) {
	*c &= ^(1 << pos)
}

func (c CapFlag) IsSet(pos int) bool {
	return c&(1<<pos) != 0
}

const (
	srvStatusSessionStateChanged = 0
)

var name2SrvStatus = map[string]int{
	"sessionStateChanged": srvStatusSessionStateChanged,
}

type srvStatus uint16

func (s *srvStatus) set(pos int) {
	*s |= 1 << pos
}

func (s *srvStatus) unSet(pos int) {
	*s &= ^(1 << pos)
}

func (s srvStatus) isSet(pos int) bool {
	return s&(1<<pos) != 0
}
