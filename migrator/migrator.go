package migrator

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"

	"github.com/Emanatry/tdsql-migrate-go/semaphore"
	"github.com/Emanatry/tdsql-migrate-go/srcreader"
)

const BATCH_SIZE = 2000
const CONCURRENT_MIGRATE_DATABASES = 7
const CONCURRENT_MIGRATE_TABLES = 1

// how many batches can happen before a `COMMIT` should be executed (assuming auto_commit=off)
const COMMIT_INTERVAL = 100

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

	if doCreateTable { // create all the tables for all the databases first
		for _, srcdb := range srca.Databases {
			for _, table := range srcdb.Tables {
				err := createTable(srcdb, table, db)
				if err != nil {
					return err
				}
			}
		}
	}

	rateLimitingSemaphore := semaphore.New(CONCURRENT_MIGRATE_DATABASES)
	var wg sync.WaitGroup
	for i, dba := range srca.Databases {
		wg.Add(1)
		rateLimitingSemaphore.Acquire()
		go func(dba *srcreader.SrcDatabase, i int) {
			if err := MigrateDatabase(dba, srcb.Databases[i], db); err != nil {
				panic(fmt.Errorf("error while migrating database [%s] from source %s:\n%s", dba.Name, dba.SrcName, err))
			}
			rateLimitingSemaphore.Release()
			defer wg.Done()
		}(dba, i)
	}
	wg.Wait()
	return nil
}

// migrate one database of a data source
func MigrateDatabase(srcdba *srcreader.SrcDatabase, srcdbb *srcreader.SrcDatabase, db *sql.DB) error {
	println("======= migrate database [" + srcdba.Name + "]")
	c := make(chan error)
	migrate := func(table string) {
		if err := MigrateTable(srcdba, srcdbb, table, db); err != nil {
			c <- fmt.Errorf("error while migrating table [%s]:\n%s", table, err)
		}
		c <- nil
	}
	// shift these around to migrate multiple tables concurrently
	concurrentTables := 0

	for i := range srcdba.Tables {
		go migrate(srcdba.Tables[i])
		concurrentTables++
		for concurrentTables >= CONCURRENT_MIGRATE_TABLES {
			err := <-c
			if err != nil {
				return err
			}
			concurrentTables--
		}
	}
	return nil
}
