package migrator

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"

	"github.com/Emanatry/tdsql-migrate-go/srcreader"
)

const BatchSize = 2000

// prepare the target instance, create `meta_migration`, etc.
func PrepareTargetDB(db *sql.DB) {
	println("preparing target db environment")

	prep, err := ioutil.ReadFile("./sql/prepare.sql")
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
		// skip empty lines
		if len(strings.TrimSpace(v)) == 0 {
			continue
		}
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
func MigrateSource(src *srcreader.Source, db *sql.DB, nodup bool) error {
	println("========== starting migration job for source " + src.SrcName)
	var wg sync.WaitGroup
	for _, srcdb := range src.Databases {
		wg.Add(1)
		go func(srcdb srcreader.SrcDatabase) {
			if err := MigrateDatabase(&srcdb, db, nodup); err != nil {
				panic(fmt.Errorf("error while migrating database [%s] from source %s:\n%s", srcdb.Name, srcdb.SrcName, err))
			}
			defer wg.Done()
		}(srcdb)

	}
	wg.Wait()
	return nil
}

// migrate one database of a data source
func MigrateDatabase(srcdb *srcreader.SrcDatabase, db *sql.DB, nodup bool) error {
	println("======= migrate database [" + srcdb.Name + "] from " + srcdb.SrcName)
	for _, table := range srcdb.Tables {
		if err := MigrateTable(srcdb, table, db, nodup); err != nil {
			return fmt.Errorf("error while migrating table [%s]:\n%s", table, err)
		}
	}
	return nil
}
