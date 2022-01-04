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
func MigrateSource(srca *srcreader.Source, srcb *srcreader.Source, db *sql.DB, doCreateTable bool) error {
	println("========== starting migration job for source " + srca.SrcName)
	var wg sync.WaitGroup
	for i, dba := range srca.Databases {
		wg.Add(1)
		go func(dba *srcreader.SrcDatabase, i int) {
			if err := MigrateDatabase(dba, srcb.Databases[i], db, doCreateTable); err != nil {
				panic(fmt.Errorf("error while migrating database [%s] from source %s:\n%s", dba.Name, dba.SrcName, err))
			}
			defer wg.Done()
		}(dba, i)

	}
	wg.Wait()
	return nil
}

// migrate one database of a data source
func MigrateDatabase(srcdba *srcreader.SrcDatabase, srcdbb *srcreader.SrcDatabase, db *sql.DB, doCreateTable bool) error {
	println("======= migrate database [" + srcdba.Name + "]")
	c := make(chan error)
	migrate := func(table string) {
		if err := MigrateTable(srcdba, srcdbb, table, db); err != nil {
			c <- fmt.Errorf("error while migrating table [%s]:\n%s", table, err)
		}
		c <- nil
	}
	// creating all tables in advance
	// since transaction is not used for insertion, there's a slight chance
	// that the table will not be present at the time of insertion on some shards,
	// resulting in an error. this reduces but *doesn't eliminate* the chance of that.
	if doCreateTable {
		for _, table := range srcdba.Tables {
			createTable(srcdba, srcdbb, table, db)
		}
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
	go migrate(srcdba.Tables[2])
	err = <-c
	if err != nil {
		return err
	}
	go migrate(srcdba.Tables[3])
	err = <-c
	if err != nil {
		return err
	}
	return nil
}
