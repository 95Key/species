package species

import (
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

const (
	driverName     = "mysql"
	dataSourceName = "root:KFxdrHt7A37n@tcp(47.100.226.101:50999)/demo?charset=utf8mb4&parseTime=True"
)

func TestNewDB(t *testing.T) {
	_, err := NewDB(driverName, dataSourceName)
	if err != nil {
		panic(err)
	}

}

func TestCreateTable(t *testing.T) {
	db, _ := NewDB(driverName, dataSourceName)
	tab := Table{
		Name: "测试创建表2",
		Columns: []Column{
			Column{
				Name:     "列1",
				DataType: "VARCHAR(128)",
			},
			Column{
				Name:     "列2",
				DataType: "VARCHAR(128)",
			},
		},
	}
	err := db.CreateTable(tab)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
}

func TestBatchInsert(t *testing.T) {
	db, _ := NewDB(driverName, dataSourceName)
	defer db.Close()
	tab := Table{
		Name: "测试创建表",
	}
	rows := [][]interface{}{
		[]interface{}{"1", "4"},
		[]interface{}{"2", "5"},
		[]interface{}{"3", "6"},
	}
	_, err := db.BatchInsert(tab, rows)
	if err != nil {
		panic(err)
	}
}
