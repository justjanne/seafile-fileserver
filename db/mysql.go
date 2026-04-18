package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MysqlDatabase struct {
	connection *sql.DB
}

func InitMysqlDatabase(config DatabaseConfig, dbName string) (*MysqlDatabase, error) {
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=%t&readTimeout=60s&writeTimeout=60s", config.User, config.Password, config.Host, config.Port, dbName, config.UseTLS)
	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)

	return &MysqlDatabase{
		connection: db,
	}, nil
}

func (db *MysqlDatabase) Connection() *sql.DB {
	return db.connection
}
