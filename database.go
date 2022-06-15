package species

import (
	"database/sql"
	"strings"

	"fmt"

	"github.com/pkg/errors"
)

var debug = false

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

// 判断数据库是否存在
func (db *DB) HasTable(table string) (bool, error) {
	sqlStr := fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? and table_name = ?;`)
	row := 0
	err := db.db.QueryRow(sqlStr, db.dbName, table).Scan(&row)
	if err != nil {
		return false, errors.WithMessage(err, "QueryRow err"+sqlStr+table)
	}
	if row == 0 {
		return false, nil
	}
	return true, nil
}

// 获取表的创建语句，使用第一行的表结构
func (t Table) CreateSQL() string {
	// CREATE TABLE `demo`.`new_table` (
	// 	`species_id` INT NOT NULL AUTO_INCREMENT,
	// 	PRIMARY KEY (`species_id`));

	colstr := "`species_id` INT NOT NULL AUTO_INCREMENT,\n"
	for _, v := range t.Columns {
		colstr += fmt.Sprintf("`%s` %s NULL,\n", v.Name, v.DataType)
	}
	colstr += "PRIMARY KEY (`species_id`)"
	return fmt.Sprintf("CREATE TABLE `%s` (\n%s\n);", t.Name, colstr)
}

// 获取表列名集合
func (t Table) GetColNameList() []string {
	ret := []string{}
	for _, col := range t.Columns {
		ret = append(ret, col.Name)
	}
	return ret
}
func (t Table) GetColNameListSafe() []string {
	ret := []string{}
	for _, col := range t.Columns {
		ret = append(ret, "`"+col.Name+"`")
	}
	return ret
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
		return errors.WithMessage(err, sqlStr)
	}
	return nil
}

func batchInsertSQL(t Table, rows [][]interface{}) string {

	表头列数 := len(t.Columns)
	tmp := strings.Repeat("?,", 表头列数)
	每行的参数占位符号 := fmt.Sprintf("(%s),", tmp[:len(tmp)-1])
	colList := strings.Join(t.GetColNameListSafe(), ",")
	tmp = strings.Repeat(每行的参数占位符号, len(rows))
	参数占位符 := tmp[:len(tmp)-1]
	return fmt.Sprintf("INSERT INTO %s(%s) VALUES %s", t.Name, colList, 参数占位符)

}

func (db *DB) batchInsert(t Table, df DataFile, rows [][]interface{}) (sql.Result, error) {
	fmt.Println("	batchInsert 小批次插入", t.Name, len(rows))
	tx, err := db.db.Begin()
	if err != nil {
		return nil, errors.WithMessage(err, "事务创建失败")
	}
	sqlStr := batchInsertSQL(t, rows)

	var values []interface{}
	// rows, err := df.GetRows(t.Name)
	// if err != nil {
	// 	return nil, errors.WithMessage(err, "batchInsert")
	// }
	for _, row := range rows {
		values = append(values, row...)
	}
	result, err := tx.Exec(sqlStr, values...)
	if err != nil {
		tx.Rollback()
		fmt.Println("insert values", values, len(values))
		return nil, errors.WithMessage(err, sqlStr)
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (db *DB) BatchInsertSheet(sheetName string, df DataFile) error {
	fmt.Println("BatchInsertSheet", sheetName)
	t, err := df.GetTable(sheetName)
	if err != nil {
		return err
	}
	rows, err := df.GetRows(sheetName)
	if err != nil {
		return err
	}
	// 去掉首行 (字段名)
	rows = rows[1:]

	// 把字段都补齐
	_, size, err := df.GetFirstRow(sheetName)
	if err != nil {
		return err
	}

	for i, row := range rows {
		for j := 0; j < size-len(row); j++ {
			rows[i] = append(rows[i], "")
		}
	}

	split := SplitRows(rows, 500)
	for _, s := range split {
		_, err := db.batchInsert(t, df, stringArrArrToInterfaceArrArr(s))
		if err != nil {
			return err
		}
	}

	return nil
}

func SplitRows(rows [][]string, size int) [][][]string {

	l := len(rows)
	if l <= size {
		return [][][]string{rows}
	}

	silce := l / size
	if l%size != 0 {
		silce++
	}
	ret := make([][][]string, silce)
	// fmt.Printf("一共 %d 行 每页 %d 行 一共 %d 页", len(rows), size, silce)

	for i := 0; i < silce; i++ {

		if len(rows) > size {
			ret[i] = rows[:size]
			rows = rows[size:]
		} else {
			ret[i] = rows
		}

	}
	return ret
}
