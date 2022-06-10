package species

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
