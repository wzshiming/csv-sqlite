//go:build !cgo

package csv_sqlite

import (
	_ "modernc.org/sqlite"
)

const (
	driverName = "sqlite"
)
