package csv_sqlite

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

func CSV2DB(ctx context.Context, f io.Reader, output string, tableName string) error {
	db, err := sql.Open(driverName, output)
	if err != nil {
		return fmt.Errorf("opening db: %w", err)
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, "PRAGMA synchronous = OFF")
	if err != nil {
		return fmt.Errorf("running WAL: %w", err)
	}

	_, err = db.ExecContext(ctx, "PRAGMA journal_mode = WAL")
	if err != nil {
		return fmt.Errorf("running WAL: %w", err)
	}

	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return fmt.Errorf("reading header row: %w", err)
	}

	createStmt := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.Join(header, ","))
	_, err = db.ExecContext(ctx, createStmt)
	if err != nil {
		return fmt.Errorf("creating table: %w (%s)", err, createStmt)
	}

	qs := strings.Repeat("?,", len(header))
	qs = qs[:len(qs)-1]
	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) values (%s)", tableName, strings.Join(header, ","), qs)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, insertStmt)
	if err != nil {
		return fmt.Errorf("prepare insert err: %w", err)
	}

	args := make([]any, len(header))
	for {
		line, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("reading row err: %w", err)
		}

		for i, v := range line {
			args[i] = v
		}

		_, err = stmt.ExecContext(ctx, args...)
		if err != nil {
			return fmt.Errorf("insert row err: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit err: %w", err)
	}

	return nil
}

func DB2CSV(ctx context.Context, input string, output io.Writer, query string) error {
	db, err := sql.Open(driverName, input)
	if err != nil {
		return fmt.Errorf("opening db: %w", err)
	}
	defer db.Close()

	r, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("running query: %w", err)
	}

	col, err := r.Columns()
	if err != nil {
		return fmt.Errorf("getting columns: %w", err)
	}

	w := csv.NewWriter(output)
	err = w.Write(col)
	if err != nil {
		return fmt.Errorf("writing row: %w", err)
	}
	defer w.Flush()

	tmp := make([]any, len(col))
	for i := range tmp {
		tmp[i] = new(string)
	}
	row := make([]string, len(col))

	for r.Next() {
		err = r.Scan(tmp...)
		if err != nil {
			return fmt.Errorf("scanning row: %w", err)
		}
		for i := range tmp {
			row[i] = *tmp[i].(*string)
		}

		err = w.Write(row)
		if err != nil {
			return fmt.Errorf("writing row: %w", err)
		}
	}
	return nil
}
