package species

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

var debug = true

type Column struct {
	OrdinalPosition int    // 字段顺序
	Name            string // 字段名称
	DataType        string // 数据类型
	Comment         string // 字段备注
	ColumnKey       string // 字段约束
}

type Table struct {
	Name    string   // 表名称
	Comment string   // 表备注
	Columns []Column // 表所含字段

	KV map[string]interface{} // 用于自定义扩展
}

type Schema struct {
	Name   string // 数据库名称
	Tables []Table
}

type DB struct {
	db *sql.DB
}

func NewDB(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return &DB{
		db: db,
	}, nil
}

// 关闭数据库连接具柄
func (db *DB) Close() error {
	return db.db.Close()
}

// 判断数据库是否存在
func (db *DB) HasTable(table string) (bool, error) {
	sqlStr := fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.TABLES WHERE table_name = ?;`)
	row := 0
	err := db.db.QueryRow(sqlStr, table).Scan(&row)
	if err != nil {
		return false, err
	}
	if row == 0 {
		return false, nil
	}
	return true, nil
}

// 获取表的创建语句，使用第一行的表结构
func (t Table) CreateSQL() string {
	colstr := ""
	for _, v := range t.Columns {
		colstr += fmt.Sprintf("`%s` %s NULL,\n", v.Name, v.DataType)
	}
	return fmt.Sprintf("CREATE TABLE `%s` (\n%s\n);", t.Name, colstr[:len(colstr)-2])
}

// 创建数据库表
func (db *DB) CreateTable(t Table) error {
	if has, _ := db.HasTable(t.Name); has {
		return errors.New(t.Name + " 该表已存在")
	}
	sqlStr := t.CreateSQL()
	if debug {
		fmt.Println(sqlStr)
	}
	_, err := db.db.Exec(sqlStr)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) BatchInsert(t Table, rows [][]interface{}) (sql.Result, error) {

	var values []interface{}
	rowsStr := ""
	for _, row := range rows {
		rStr := strings.Repeat("?,", len(row))
		rowsStr += fmt.Sprintf("(%s),\n", rStr[:len(rStr)-1])

		values = append(values, row...)
	}
	fmt.Println(values)

	sqlStr := fmt.Sprintf("INSERT INTO %s VALUES %s", t.Name, rowsStr[:len(rowsStr)-2])

	tx, _ := db.db.Begin()
	result, err := tx.Exec(sqlStr, values...)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return result, nil
}
