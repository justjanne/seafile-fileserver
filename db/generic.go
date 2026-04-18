package db

import (
	"database/sql"
	"fmt"
)

type Database interface {
	Connection() *sql.DB
}

func InitDatabase(config DatabaseConfig, dbName string) (Database, error) {
	switch config.Type {
	case "mysql":
		return InitMysqlDatabase(config, dbName)
	case "pgsql":
		return InitPgsqlDatabase(config, dbName)
	default:
		return nil, fmt.Errorf("failed to open database: unknown type %s", config.Type)
	}
}
