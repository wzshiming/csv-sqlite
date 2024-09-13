package main

import (
	"io"
	"log"
	"os"

	"github.com/spf13/cobra"

	csv_sqlite "github.com/wzshiming/csv-sqlite"
)

var (
	tableName   = "csv"
	dbFileName  = "csv.db"
	csvFileName = ""
	sql         = "SELECT * FROM csv"
)

var (
	cmdRoot = cobra.Command{
		Use: "csv-sqlite",
	}
	cmdToDB = cobra.Command{
		Use:  "to-db",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var r io.Reader
			if csvFileName == "" {
				r = os.Stdin
			} else {
				f, err := os.Open(csvFileName)
				if err != nil {
					return err
				}
				defer f.Close()
				r = f
			}

			return csv_sqlite.CSV2DB(cmd.Context(), r, dbFileName, tableName)
		},
	}
	cmdToCSV = cobra.Command{
		Use:  "to-csv",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var w io.Writer
			if csvFileName == "" {
				w = os.Stdout
			} else {
				f, err := os.Create(csvFileName)
				if err != nil {
					return err
				}
				defer f.Close()
				w = f
			}

			return csv_sqlite.DB2CSV(cmd.Context(), dbFileName, w, sql)
		},
	}
)

func init() {
	cmdToDB.Flags().StringVar(&tableName, "table", tableName, "table name")
	cmdToCSV.Flags().StringVar(&sql, "sql", sql, "sql statement")
	cmdRoot.PersistentFlags().StringVar(&dbFileName, "db", dbFileName, "database file path")
	cmdRoot.PersistentFlags().StringVar(&csvFileName, "csv", csvFileName, "csv file path")

	cmdRoot.AddCommand(&cmdToDB)
	cmdRoot.AddCommand(&cmdToCSV)
}

func main() {
	if err := cmdRoot.Execute(); err != nil {
		log.Fatal(err)
	}
}
