//go:build cgo

package csv_sqlite

import (
	_ "github.com/mattn/go-sqlite3"
)

const (
	driverName = "sqlite3"
)
