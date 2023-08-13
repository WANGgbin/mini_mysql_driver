package mysql

import (
	"database/sql"
	"fmt"
	"github.com/smartystreets/goconvey/convey"
	"net"
	"testing"
	"time"
)

func TestMysql(t *testing.T) {
	convey.Convey("", t, func(){
		dsn := "test:123456@tcp(127.0.0.1:3306)/world"
		db, err := sql.Open("mini_mysql", dsn)
		convey.So(err, convey.ShouldBeNil)

		//err = db.Ping()
		//convey.So(err, convey.ShouldBeNil)

		//rows, err := db.QueryContext(context.Background(), "select name, gender, secret, token, extra, age, born_time from person" )
		//convey.So(err, convey.ShouldBeNil)

		type Person struct {
			Name string
			Gender string
			IsAlive bool
			Secret []byte
			Token []byte
			Extra []byte
			// 可能为 null 的字段一定要设置为指针类型
			Age *int16
			BornTime time.Time
		}

		//defer func() {
		//	_ = rows.Close()
		//}()
		//for rows.Next() {
		//	var p Person
		//	err = rows.Scan(&p.Name, &p.Gender, &p.Secret, &p.Token, &p.Secret, &p.Age, &p.BornTime)
		//	convey.So(err, convey.ShouldBeNil)
		//	t.Logf("%v, %#X", p.Name, p.Token)
		//}

		//ret, err := db.ExecContext(context.Background(), "insert into person (name, gender, age, secret, is_alive, born_time) values(?, ?, ?, ?, ?, ?)", "小美", "female", nil, "xxxxxx", true, time.Now())
		//convey.So(err, convey.ShouldBeNil)
		//lastInserID, err := ret.LastInsertId()
		//convey.So(err, convey.ShouldBeNil)
		//affectedRows, err := ret.RowsAffected()
		//convey.So(err, convey.ShouldBeNil)
		//
		//t.Logf("lastInsertID： %d, affectedRows: %d", lastInserID, affectedRows)

		tx, err := db.Begin()
		if err != nil {
			fmt.Printf("tx begin error: %v\n", err)
			return
		}

		ret, err := tx.Exec("insert into city(name, countrycode, district, population) values(?, ?, ?, ?)", "Beijing", "CHN", "Beijing", 10000000)
		if err != nil {
			tx.Rollback()
			fmt.Printf("insert error: %v, rollback!", err)
			return
		}
		_, _ = tx.Exec("desc person")
		tx.Commit()
		id, _ := ret.LastInsertId()
		fmt.Printf("new id: %d\n", id)
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