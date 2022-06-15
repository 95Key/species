package species

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

type DB struct {
	db       *sql.DB
	ip       string
	port     string
	dbName   string
	user     string
	password string
}

type Schema struct {
	Name   string // 数据库名称
	Tables []Table
}

func NewDB(driverName, ip, port, user, password, dbName string) (*DB, error) {
	dataSourceName := dataSourceName(ip, port, user, password, dbName)
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, errors.WithMessage(err, "Open db err")
	}
	if err = db.Ping(); err != nil {
		return nil, errors.WithMessage(err, "Ping db error")
	}
	return &DB{
		db:       db,
		ip:       ip,
		port:     port,
		dbName:   dbName,
		user:     user,
		password: password,
	}, nil
}

func dataSourceName(ip, port, user, password, db string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True", user, password, ip, port, db)
}

// 关闭数据库连接具柄
func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Exec(query string, args ...any) (sql.Result, error) {
	return db.db.Exec(query, args...)
}
