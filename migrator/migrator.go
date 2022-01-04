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
func MigrateSource(srca *srcreader.Source, srcb *srcreader.Source, db *sql.DB, nodup bool) error {
	println("========== starting migration job for source " + srca.SrcName)
	var wg sync.WaitGroup
	for i, dba := range srca.Databases {
		wg.Add(1)
		go func(dba *srcreader.SrcDatabase, i int) {
			if err := MigrateDatabase(dba, srcb.Databases[i], db, nodup); err != nil {
				panic(fmt.Errorf("error while migrating database [%s] from source %s:\n%s", dba.Name, dba.SrcName, err))
			}
			defer wg.Done()
		}(dba, i)

	}
	wg.Wait()
	return nil
}

// migrate one database of a data source
func MigrateDatabase(srcdba *srcreader.SrcDatabase, srcdbb *srcreader.SrcDatabase, db *sql.DB, nodup bool) error {
	println("======= migrate database [" + srcdba.Name + "]")
	c := make(chan error)
	migrate := func(table string) {
		if err := MigrateTable(srcdba, srcdbb, table, db, nodup); err != nil {
			c <- fmt.Errorf("error while migrating table [%s]:\n%s", table, err)
		}
		c <- nil
	}
	// shift these around to migrate multiple tables concurrently
	go migrate(srcdba.Tables[0])
	err := <-c
	if err != nil {
		return err
	}
	go migrate(srcdba.Tables[1])
	err = <-c
	if err != nil {
		return err
	}
	go migrate(srcdba.Tables[3]) // migrate table 4 before 3, to allow more time for key rebuild
	err = <-c
	if err != nil {
		return err
	}
	go migrate(srcdba.Tables[2])
	err = <-c
	if err != nil {
		return err
	}
	return nil
}
