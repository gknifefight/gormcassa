package gorm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type commonDialect struct {
	driver string
	dsn    string
	db     sqlCommon
}

func NewCommonDialect(driver string, dsn string) (commonDialect, error) {
	return commonDialect{driver: driver, dsn: dsn}, nil
}

func (c commonDialect) Connect() error {
	db, err := sql.Open(c.driver, c.dsn)
	c.db = db

	return err
}

func (c commonDialect) clone() Dialect {
	return commonDialect{
		driver: c.driver,
		dsn:    c.dsn,
		db:     c.db,
	}
}

func (c commonDialect) RollbackTransaction() error {
	if db, ok := c.db.(sqlTx); ok {
		err := db.Rollback()

		if err != nil {
			return err
		}

		return nil
	}

	return NoValidTransaction
}

func (c commonDialect) BeginTransaction() error {
	if db, ok := c.db.(sqlDb); ok {
		tx, err := db.Begin()

		if err != nil {
			return err
		}

		c.db = interface{}(tx).(sqlCommon)

		return nil
	}

	return CantStartTransaction
}

func (c commonDialect) CommitTransaction() error {
	if db, ok := c.db.(sqlTx); ok {
		err := db.Commit()

		if err != nil {
			return err
		}

		return nil
	}

	return NoValidTransaction
}

func (c commonDialect) DB() *sql.DB {
	return c.db.(*sql.DB)
}

func (c commonDialect) CloseDB() error {
	return c.DB().Close()
}

func (commonDialect) BinVar(i int) string {
	return "$$" // ?
}

func (commonDialect) SupportLastInsertId() bool {
	return true
}

func (commonDialect) HasTop() bool {
	return false
}

func (commonDialect) SqlTag(value reflect.Value, size int, autoIncrease bool) string {
	switch value.Kind() {
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
		if autoIncrease {
			return "INTEGER AUTO_INCREMENT"
		}
		return "INTEGER"
	case reflect.Int64, reflect.Uint64:
		if autoIncrease {
			return "BIGINT AUTO_INCREMENT"
		}
		return "BIGINT"
	case reflect.Float32, reflect.Float64:
		return "FLOAT"
	case reflect.String:
		if size > 0 && size < 65532 {
			return fmt.Sprintf("VARCHAR(%d)", size)
		}
		return "VARCHAR(65532)"
	case reflect.Struct:
		if _, ok := value.Interface().(time.Time); ok {
			return "TIMESTAMP"
		}
	default:
		if _, ok := value.Interface().([]byte); ok {
			if size > 0 && size < 65532 {
				return fmt.Sprintf("BINARY(%d)", size)
			}
			return "BINARY(65532)"
		}
	}
	panic(fmt.Sprintf("invalid sql type %s (%s) for commonDialect", value.Type().Name(), value.Kind().String()))
}

func (commonDialect) ReturningStr(tableName, key string) string {
	return ""
}

func (commonDialect) SelectFromDummyTable() string {
	return ""
}

func (commonDialect) Quote(key string) string {
	return fmt.Sprintf(`"%s"`, key)
}

func (commonDialect) databaseName(scope *Scope) string {
	from := strings.Index(scope.db.parent.source, "/") + 1
	to := strings.Index(scope.db.parent.source, "?")
	if to == -1 {
		to = len(scope.db.parent.source)
	}
	return scope.db.parent.source[from:to]
}

func (c commonDialect) HasTable(scope *Scope, tableName string) bool {
	var count int
	scope.NewDB().Raw("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_name = ? AND table_schema = ?", tableName, c.databaseName(scope)).Row().Scan(&count)
	return count > 0
}

func (c commonDialect) HasColumn(scope *Scope, tableName string, columnName string) bool {
	var count int
	scope.NewDB().Raw("SELECT count(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ? AND column_name = ?", c.databaseName(scope), tableName, columnName).Row().Scan(&count)
	return count > 0
}

func (commonDialect) HasIndex(scope *Scope, tableName string, indexName string) bool {
	var count int
	scope.NewDB().Raw("SELECT count(*) FROM INFORMATION_SCHEMA.STATISTICS where table_name = ? AND index_name = ?", tableName, indexName).Row().Scan(&count)
	return count > 0
}

func (commonDialect) RemoveIndex(scope *Scope, indexName string) {
	scope.NewDB().Exec(fmt.Sprintf("DROP INDEX %v ON %v", indexName, scope.QuotedTableName()))
}
