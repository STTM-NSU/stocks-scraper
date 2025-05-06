package postgres

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type Config struct {
	Host     string
	Port     string
	Username string
	Password string
	DBName   string
	SSLMode  string
}

func NewConfigFromEnv() *Config {
	return &Config{
		Host:     os.Getenv("POSTGRES_HOST"),
		Port:     os.Getenv("POSTGRES_PORT"),
		Username: os.Getenv("POSTGRES_USERNAME"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
		DBName:   os.Getenv("POSTGRES_DB_NAME"),
		SSLMode:  os.Getenv("POSTGRES_SSL_MODE"),
	}
}

func (c *Config) Setup() *Config {
	const (
		defaultHost     = "localhost"
		defaultPort     = "5432"
		defaultUsername = "postgres"
		defaultPassword = "postgres"
		defaultDBName   = "postgres"
		defaultSSLMode  = "disable"
	)

	c.Host = cmp.Or(c.Host, defaultHost)
	c.Port = cmp.Or(c.Port, defaultPort)
	if _, err := strconv.Atoi(c.Port); err != nil {
		c.Port = defaultPort
	}
	c.Username = cmp.Or(c.Username, defaultUsername)
	c.Password = cmp.Or(c.Password, defaultPassword)
	c.DBName = cmp.Or(c.DBName, defaultDBName)
	c.SSLMode = cmp.Or(c.SSLMode, defaultSSLMode)

	return c
}

func (c *Config) String() string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s dbname=%s password=%s sslmode=%s",
		c.Host, c.Port, c.Username, c.DBName, c.Password, c.SSLMode,
	)
}

func NewDB(cfg *Config) (*sqlx.DB, error) {
	return sqlx.Connect("postgres", cfg.String())
}

func PrepareStocksStmt(ctx context.Context, db *sqlx.DB) (*sql.Stmt, error) {
	return db.PrepareContext(ctx, "INSERT INTO stocks (instrument_id, ts, close_price) VALUES ($1, $2, $3)")
}

func PrepareInstrumentsStmt(ctx context.Context, db *sqlx.DB) (*sql.Stmt, error) {
	return db.PrepareContext(ctx, "INSERT INTO instruments (id, first_candle_date) VALUES ($1, $2)")
}
