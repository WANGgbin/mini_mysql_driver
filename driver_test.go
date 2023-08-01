package mysql

import (
	"context"
	"database/sql"
	"github.com/smartystreets/goconvey/convey"
	"net"
	"testing"
)

func TestMysql(t *testing.T) {
	convey.Convey("", t, func(){
		dsn := "test:123456@tcp(127.0.0.1:3306)/world"
		db, err := sql.Open("mini_mysql", dsn)
		convey.So(err, convey.ShouldBeNil)

		err = db.Ping()
		convey.So(err, convey.ShouldBeNil)

		db.QueryContext(context.Background(), "sql", )
	})
}

func Test_parseDsn(t *testing.T) {
	convey.Convey("", t, func(){
		testCases := []struct {
			name string
			dsn string
			want *dbInfo
			shouldErr bool
		}{
			{
				name: "miss user",
				dsn: "1234@tcp(127.0.0.1:3306)/world",
				shouldErr: true,
			},
			{
				name: "right",
				dsn: "name:1234@tcp(127.0.0.1:3306)/world",
				want: &dbInfo{
					ip: net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 127, 0, 0, 1},
					port: 3306,
					protocol: "tcp",
					user: "name",
					password: "1234",
					dbName: "world",
				},
			},
		}

		for _, testCase := range testCases {
			convey.Convey(testCase.name, func(){
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