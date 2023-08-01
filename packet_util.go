package mysql

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
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

func (prw *pktReadWriter) read(pkt interface{}) error {
	pk, err := prw.doRead()
	if err != nil {
		return err
	}
	prw.conn.curSeqID = pk.seqID + 1
	return extractPkt(&pk.payload, pkt, &prw.conn.capFlag)
}

func (prw *pktReadWriter) readOkPkt() (*OkPacket, error) {
	rawPkt, err := prw.doRead()
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
		return nil, fmt.Errorf("receive unknown packet stating with [%#X]", rawPkt.payload[0])
	}

	err = extractPkt(&rawPkt.payload, pkt, &prw.conn.capFlag)
	if err != nil {
		return nil, fmt.Errorf("extrack pkt error: %v", err)
	}

	if errPkt, ok := pkt.(*ErrPacket); ok {
		return nil, fmt.Errorf("recieve err pkt from server: code: %d, msg: %s", errPkt.ErrCode, errPkt.ErrMsg)
	}

	prw.conn.curSeqID = 0
	return pkt.(*OkPacket), nil
}

func (prw *pktReadWriter) readAuthResult() (interface{}, error) {
	rawPkt, err := prw.doRead()
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

	prw.conn.curSeqID = rawPkt.seqID + 1
	return pkt, nil
}

func (prw *pktReadWriter) readStmtPrepareResp() (*StmtPrepareOK, error) {
	rawPkt, err := prw.doRead()
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
		return nil, fmt.Errorf("receive unknown packet stating with [%#X] after sending prepare", rawPkt.payload[0])
	}

	err = extractPkt(&rawPkt.payload, pkt, &prw.conn.capFlag)
	if err != nil {
		return nil, fmt.Errorf("extrack prepare resp pkt error: %v", err)
	}

	if errPkt, ok := pkt.(*ErrPacket); ok {
		return nil, fmt.Errorf("when prepareing, recieve err pkt from server: code: %d, msg: %s", errPkt.ErrCode, errPkt.ErrMsg)
	}

	prw.conn.curSeqID = rawPkt.seqID + 1
	return pkt.(*StmtPrepareOK), nil
}

func (prw *pktReadWriter) doRead() (*Packet, error) {
	pk := new(Packet)

	lenRaw := make([]byte, 3)
	err := prw.mrw.read(lenRaw)
	if err != nil {
		return nil, err
	}

	length, err := extractInt(&lenRaw, 3)
	if err != nil {
		return nil, err
	}
	pk.length = uint32(length)

	seqIDRaw := make([]byte, 1)
	err = prw.mrw.read(seqIDRaw)
	if err != nil {
		return nil, err
	}

	seqID, err := extractInt(&seqIDRaw, 1)
	if err != nil {
		return nil, err
	}
	pk.seqID = uint8(seqID)

	data := make([]byte, pk.length)
	err = prw.mrw.read(data)
	if err != nil {
		return nil, err
	}
	pk.payload = data

	return pk, nil
}

func (prw *pktReadWriter) Write(pkt interface{}, seqID uint8) error {
	buf := bytes.Buffer{}
	payload := marshalPkt(pkt, &prw.conn.capFlag)
	buf.Write(marshal(uint64(len(payload)), 3))
	buf.Write(marshal(uint64(seqID), 1))
	buf.Write(payload)

	data := buf.Bytes()

	err := prw.mrw.write(data)
	if err != nil {
		return fmt.Errorf("send packet error: %v", err)
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

func (m *mustReadWriter) write(p []byte) error {
	n, err := m.brw.Write(p)
	if err != nil {
		return fmt.Errorf("write error: %v", err)
	}
	if len(p) != n {
		return fmt.Errorf("write %d bytes, expected: %d bytes", n, len(p))
	}

	err = m.brw.Flush()
	if err != nil {
		return fmt.Errorf("flush buf error: %v", err)
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
			buf.Write(marshal(data, 1))
		case "2":
			data := fieldVal.Uint()
			buf.Write(marshal(data, 2))
		case "3":
			data := fieldVal.Uint()
			buf.Write(marshal(data, 3))
		case "4":
			data := fieldVal.Uint()
			buf.Write(marshal(data, 4))
		case "6":
			data := fieldVal.Uint()
			buf.Write(marshal(data, 6))
		case "8":
			data := fieldVal.Uint()
			buf.Write(marshal(data, 8))
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
			buf.Write(marshalVarInt(uint64(len(data))))
			buf.WriteString(data)
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

func marshal(val uint64, length int) []byte {
	ret := make([]byte, length)

	for idx := 0; idx < length; idx++ {
		ret[length-1-idx] = byte(val >> (8 * (length - 1 - idx)))
	}

	return ret
}

var (
	ErrLessLength = errors.New("less length")
)

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

func marshalVarInt(v uint64) []byte {
	if v < 251 {
		return marshal(v, 1)
	}

	if v < 2^16 {
		return append([]byte{0xFC}, marshal(v, 2)...)
	}

	if v < 2^24 {
		return append([]byte{0xFD}, marshal(v, 3)...)
	}

	return append([]byte{0xFE}, marshal(v, 8)...)
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

	ret := string((*dataPtr)[:length])
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

// 参考：https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html
const (
	CapClientLongPassword     = 0
	CapClientColumnLongFlag   = 2
	CapClientProtocol41       = 9
	CapClientConnectWithDB    = 3
	CapClientPluginAuth       = 19
	CapClientTransactions     = 13
	CapClientSessionTrack     = 23
	CapClientMultiResults     = 16
	CapClientDeprecateEof     = 24
	CapClientAuthentication41 = 15
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
