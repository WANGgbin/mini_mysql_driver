package mysql

import (
	"github.com/smartystreets/goconvey/convey"
	"net"
	"testing"
)

func Test_parseDsn(t *testing.T) {
	convey.Convey("", t, func() {
		testCases := []struct {
			name      string
			dsn       string
			want      *dbCfg
			shouldErr bool
		}{
			{
				name:      "miss user",
				dsn:       "1234@tcp(127.0.0.1:3306)/world",
				shouldErr: true,
			},
			{
				name: "right",
				dsn:  "name:1234@tcp(127.0.0.1:3306)/world",
				want: &dbCfg{
					dbCfgPath: &dbCfgPath{
						ip:       net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 0, 0, 1},
						port:     3306,
						protocol: "tcp",
						user:     "name",
						password: "1234",
						dbName:   "world",
					},
				},
			},
		}

		for _, testCase := range testCases {
			convey.Convey(testCase.name, func() {
				gotDbInfo, gotErr := parseDsn(testCase.dsn)
				if testCase.shouldErr {
					convey.So(gotErr, convey.ShouldNotBeNil)
				} else {
					convey.So(gotDbInfo, convey.ShouldResemble, testCase.want)
				}
			})
		}
	})
}