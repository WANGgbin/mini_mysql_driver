package mysql

import (
	"fmt"
	"github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func Test_extractPkt(t *testing.T) {
	convey.Convey("", t, func(){
		testCases := []struct{
			data []byte
			pkt interface{}
			flag CapFlag
		}{
			{
				data: []byte{0x0a, 0x38, 0x2e, 0x30, 0x2e, 0x33, 0x33, 0x0, 0xa, 0x0, 0x0, 0x0, 0x7a, 0x75, 0x32, 0x16, 0x68, 0x45, 0x1c, 0x7c, 0x0, 0xff, 0xff, 0xff, 0x2, 0x0, 0xff, 0xdf, 0x15, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x23, 0x5, 0x4, 0x36, 0x70, 0x16, 0x54, 0x75, 0x27, 0x42, 0x67, 0x0, 0x63, 0x61, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x5f, 0x73, 0x68, 0x61, 0x32, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x0},
				pkt: new(HandshakeV10),
				flag: CapFlag(0xffffffff),
			},
		}

		for _, testCase := range testCases {
			t.Logf("len of data: %d", len(testCase.data))
			err := extractPkt(&testCase.data, testCase.pkt, &testCase.flag)
			convey.So(err, convey.ShouldBeNil)
		}
	})
}

func Test_satisfyCap(t *testing.T) {
	convey.Convey("", t, func(){
		testCases := []struct{
			flag CapFlag
			cond string
			want bool
		}{
			{
				flag: newCapFlag(CapClientProtocol41),
				cond: "protocol41",
				want: true,
			},
			{
				flag: newCapFlag(CapClientProtocol41),
				cond: "!protocol41",
				want: false,
			},
			{
				flag: 0,
				cond: "!protocol41",
				want: true,
			},
			{
				flag: newCapFlag(CapClientConnectWithDB),
				cond: "protocol41||connectWithDb",
				want: true,
			},
			{
				flag: newCapFlag(CapClientConnectWithDB),
				cond: "protocol41||pluginAuth",
				want: false,
			},
			{
				flag: newCapFlag(CapClientConnectWithDB),
				cond: "protocol41||pluginAuth||connectWithDb",
				want: true,
			},
		}

		for _, testCase := range testCases {
			got := satisfyCap(testCase.flag, testCase.cond)
			convey.So(got, convey.ShouldEqual, testCase.want)
		}
	})
}

func Test_marshalTime(t *testing.T) {
	for i := 0; i < 10; i++ {
		fmt.Printf("%s\n", string(marshalTime(time.Now())))
		time.Sleep(1 * time.Second)
	}
}

func Test_extractMysqlDouble(t *testing.T) {
	data := []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x24, 0x40}
	d, err := extractMysqlTypeDouble(&data)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(d)
}

func Test_extractMysqlFloat(t *testing.T) {
	data := []byte{0x33, 0x33, 0x23, 0x41}
	d, err := extractMysqlTypeFloat(&data)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(d)
}

func Test_extractMysqlTypeDate(t *testing.T) {
	convey.Convey("", t, func(){
		testCases := []struct{
			data []byte
			want string
		}{
			{
				data: []byte{0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00},
				want: "2010-10-17T19:27:30.000001+08:00",
			},
		}

		for _, testCase := range testCases {
			t, err := doExtractMysqlTypeDate(&testCase.data)
			convey.So(err, convey.ShouldBeNil)

			convey.So(t.Format(time.RFC3339Nano), convey.ShouldEqual, testCase.want)
		}
	})
}