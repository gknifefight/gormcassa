package gorm

import (
	"fmt"
	"reflect"
)

type Dialect interface {
	clone() Dialect
	DB() Database
	Exec(query string, vars ...interface{}) (Result, error)
	Query(query string, vars ...interface{}) (Rows, error)
	QueryRow(query string, vars ...interface{}) Row
	RollbackTransaction() error
	Connect() error
	BeginTransaction() error
	CommitTransaction() error
	CloseDB() error
	BinVar(i int) string
	SupportLastInsertId() bool
	HasTop() bool
	SqlTag(value reflect.Value, size int, autoIncrease bool) string
	ReturningStr(tableName, key string) string
	SelectFromDummyTable() string
	Quote(key string) string
	HasTable(scope *Scope, tableName string) bool
	HasColumn(scope *Scope, tableName string, columnName string) bool
	HasIndex(scope *Scope, tableName string, indexName string) bool
	RemoveIndex(scope *Scope, indexName string)
}

func NewDialect(driver string, dsn string) (Dialect, error) {
	var d Dialect
	var err error

	switch driver {
	case "postgres":
		common, err := NewCommonDialect(driver, dsn)

		if err != nil {
			return nil, err
		}

		d = &postgres{common}
	case "cassandra":
		d, err = NewCassandraDialect(dsn)
	default:
		fmt.Printf("`%v` is not officially supported, running under compatibility mode.\n", driver)
		d = &commonDialect{}
	}

	err = d.Connect()

	return d, err
}
