package mysql

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestMysql(t *testing.T) {
	convey.Convey("", t, func(){
		dsn := "test:123456@tcp(127.0.0.1:3306)/world"
		db, err := sql.Open("mysql", dsn)
		convey.So(err, convey.ShouldBeNil)

		err = db.Ping()
		convey.So(err, convey.ShouldBeNil)

		rows, err := db.QueryContext(context.Background(), "select name, gender, secret, token, extra, age, born_time from person where id = 1" )
		convey.So(err, convey.ShouldBeNil)
		sql.NullTime{}
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

		defer func() {
			_ = rows.Close()
		}()
		for rows.Next() {
			var p Person
			err = rows.Scan(&p.Name, &p.Gender, &p.Secret, &p.Token, &p.Secret, &p.Age, &p.BornTime)
			convey.So(err, convey.ShouldBeNil)
			t.Logf("%v, %#X, %s", p.Name, p.Token, p.BornTime.Format(time.RFC3339))
		}

		//ret, err := db.ExecContext(context.Background(), "insert into person (name, gender, age, secret, is_alive, born_time) values(?, ?, ?, ?, ?, ?)", "小美", "female", nil, []byte{0x01, 0x02}, true, time.Now())
		//convey.So(err, convey.ShouldBeNil)
		//lastInserID, err := ret.LastInsertId()
		//convey.So(err, convey.ShouldBeNil)
		//affectedRows, err := ret.RowsAffected()
		//convey.So(err, convey.ShouldBeNil)
		//t.Logf("lastInsertID： %d, affectedRows: %d", lastInserID, affectedRows)
		//
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
