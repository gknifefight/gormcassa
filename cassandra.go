package gorm

import (
	"database/sql"
	"fmt"
	"github.com/gocql/gocql"
	"reflect"
	"strings"
	"time"
)

type cassandra struct {
	Cluster *gocql.ClusterConfig
	Session *gocql.Session
	dsn     dsn
}

type dsn struct {
	keyspace string
	hosts    []string
}

func parseDSN(source string) (dsn, error) {
	result := dsn{}

	for _, part := range strings.Split(source, " ") {
		parts := strings.Split(part, "=")

		if parts[0] == "keyspace" {
			result.keyspace = parts[1]
		}

		if parts[0] == "hosts" {
			result.hosts = strings.Split(parts[1], ",")
		}
	}

	return result, nil
}

func NewCassandraDialect(source string) (Dialect, error) {
	dsn, err := parseDSN(source)

	if err != nil {
		return nil, err
	}

	cass := &cassandra{
		dsn: dsn,
	}

	return cass, nil
}

func (c cassandra) clone() Dialect {
	return cassandra{
		dsn:     c.dsn,
		Cluster: c.Cluster,
		Session: c.Session,
	}
}

func (c cassandra) Connect() error {
	cluster := gocql.NewCluster(c.dsn.hosts...)
	cluster.Keyspace = c.dsn.keyspace

	c.Cluster = cluster

	session, err := cluster.CreateSession()

	if err != nil {
		return err
	}

	c.Session = session

	return nil
}

func (cassandra) RollbackTransaction() error {
	return TransactionNotSupported
}

func (cassandra) BeginTransaction() error {
	return TransactionNotSupported
}

func (cassandra) CommitTransaction() error {
	return TransactionNotSupported
}

func (c cassandra) CloseDB() error {
	c.Session.Close()

	return nil
}

func (cassandra) DB() *sql.DB {
	return nil
}

func (cassandra) ReturningStr(tableName, key string) string {
	return ""
}

func (cassandra) SelectFromDummyTable() string {
	return ""
}

func (cassandra) Quote(key string) string {
	return fmt.Sprintf(`"%s"`, key)
}

func (cassandra) SupportLastInsertId() bool {
	return false
}

func (c cassandra) databaseName(scope *Scope) string {
	return c.Cluster.Keyspace
}

func (cassandra) HasTop() bool {
	return false
}

func (cassandra) BinVar(i int) string {
	return "$$" // ?
}

func (cassandra) SqlTag(value reflect.Value, size int, autoIncrease bool) string {
	switch value.Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
		return "int"
	case reflect.Int64, reflect.Uint64:
		return "bigint"
	case reflect.Float32, reflect.Float64:
		return "float"
	case reflect.String:
		return "varchar"
	case reflect.Struct:
		if _, ok := value.Interface().(time.Time); ok {
			return "timestamp"
		}
	default:
		if _, ok := value.Interface().([]byte); ok {
			return "blob"
		}
	}
	panic(fmt.Sprintf("invalid sql type %s (%s) for cassandra", value.Type().Name(), value.Kind().String()))
}

func (cassandra) HasTable(scope *Scope, tableName string) bool {
	var count int
	scope.NewDB().Raw("SELECT count(*) FROM INFORMATION_SCHEMA.tables WHERE table_name = ? AND table_type = 'BASE TABLE'", tableName).Row().Scan(&count)
	return count > 0
}

func (cassandra) HasColumn(scope *Scope, tableName string, columnName string) bool {
	var count int
	scope.NewDB().Raw("SELECT count(*) FROM INFORMATION_SCHEMA.columns WHERE table_name = ? AND column_name = ?", tableName, columnName).Row().Scan(&count)
	return count > 0
}

func (cassandra) RemoveIndex(scope *Scope, indexName string) {
	scope.NewDB().Exec(fmt.Sprintf("DROP INDEX %v", indexName))
}

func (cassandra) HasIndex(scope *Scope, tableName string, indexName string) bool {
	var count int
	scope.NewDB().Raw("SELECT count(*) FROM pg_indexes WHERE tablename = ? AND indexname = ?", tableName, indexName).Row().Scan(&count)
	return count > 0
}
