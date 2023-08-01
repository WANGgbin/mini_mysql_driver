package mysql

import (
	"github.com/smartystreets/goconvey/convey"
	"testing"
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

