package gorm

import (
	"fmt"
	"github.com/gocql/gocql"
	"reflect"
	"strings"
	"time"
)

type cassandra struct {
	commonDialect
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

func (cassandra) Open(driver string, source string) (*gocql.Session, error) {
	dsn, err := parseDSN(source)

	if err != nil {
		return nil, err
	}

	cluster := gocql.NewCluster(dsn.hosts...)
	cluster.Keyspace = dsn.keyspace

	return cluster.CreateSession()
}

func (cassandra) SupportLastInsertId() bool {
	return false
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
