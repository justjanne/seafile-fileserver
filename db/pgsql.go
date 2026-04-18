package db

import (
	"database/sql"
	"time"

	"github.com/lib/pq"
)

type PgsqlDatabase struct {
	connection *sql.DB
}

func InitPgsqlDatabase(config DatabaseConfig, dbName string) (*PgsqlDatabase, error) {
	var sslMode pq.SSLMode
	if config.UseTLS {
		sslMode = pq.SSLModeVerifyFull
	} else {
		sslMode = pq.SSLModeDisable
	}
	connector, err := pq.NewConnectorConfig(pq.Config{
		Host:           config.Host,
		Port:           config.Port,
		User:           config.User,
		Password:       config.Password,
		Database:       dbName,
		ConnectTimeout: 5 * time.Second,
		SSLMode:        sslMode,
	})
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)

	return &PgsqlDatabase{
		connection: db,
	}, nil
}

func (db *PgsqlDatabase) Connection() *sql.DB {
	return db.connection
}
