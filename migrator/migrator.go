package migrator

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/Emanatry/tdsql-migrate-go/srcreader"
)

// prepare the target instance, create `meta_migration`, etc.
func PrepareTargetDB(db *sql.DB) {
	println("preparing target db environment")

	prep, err := os.ReadFile("./sql/prepare.sql")
	if err != nil {
		panic("failed reading prepare.sql: " + err.Error())
	}

	stmts := strings.Split(string(prep), ";")

	totalRowsAffected := 0

	tx, err := db.Begin()
	if err != nil {
		panic("failed creating transaction " + err.Error())
	}

	for _, v := range stmts {
		result, err := tx.Exec(v + ";")
		if err != nil {
			panic("failed executing prepare.sql: " + err.Error())
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			panic("failed getting rowsAffected: " + err.Error())
		}
		totalRowsAffected += int(rowsAffected)
	}

	if err := tx.Commit(); err != nil {
		panic("failed commiting transaction: " + err.Error())
	}

	fmt.Printf("prepare.sql finished. totalRowsAffected: %d\n", totalRowsAffected)
}

// migrate a whole data source
func MigrateSource(src *srcreader.Source, db *sql.DB) error {
	println("* starting migration job for source " + src.SrcName)
	for _, srcdb := range src.Databases {
		if err := migrateDatabase(&srcdb, db); err != nil {
			return fmt.Errorf("error while migrating database [%s]:\n%s", srcdb.Name, err)
		}
	}
	return nil
}

// migrate one database of a data source
func migrateDatabase(srcdb *srcreader.SrcDatabase, db *sql.DB) error {
	println("migrate database [" + srcdb.Name + "] from " + srcdb.SrcName)
	for _, table := range srcdb.Tables {
		if err := migrateTable(srcdb, table, db); err != nil {
			return fmt.Errorf("error while migrating table [%s]:\n%s", table, err)
		}
	}
	return nil
}

// migrate one table from a source database
func migrateTable(srcdb *srcreader.SrcDatabase, tablename string, db *sql.DB) error {
	println("migrating table " + tablename + " from database " + srcdb.Name + " from " + srcdb.SrcName)
	_, err := srcdb.OpenCSV(tablename)
	if err != nil {
		return err
	}

	// unimplemented

	return nil
}
