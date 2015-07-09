package gorm

import (
	"database/sql"
	"fmt"
	"reflect"
)

type Dialect interface {
	RollbackTransaction() error
	BeginTransaction() error
	CommitTransaction() error
	DB() *sql.DB
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

	return d, err
}
