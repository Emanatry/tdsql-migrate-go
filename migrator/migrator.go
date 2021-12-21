package migrator

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/Emanatry/tdsql-migrate-go/srcreader"
	"github.com/Emanatry/tdsql-migrate-go/stats"
)

const BatchSize = 3000

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
	println("========== starting migration job for source " + src.SrcName)
	var wg sync.WaitGroup
	for _, srcdb := range src.Databases {
		wg.Add(1)
		go func(srcdb srcreader.SrcDatabase) {
			if err := MigrateDatabase(&srcdb, db); err != nil {
				panic(fmt.Errorf("error while migrating database [%s] from source %s:\n%s", srcdb.Name, srcdb.SrcName, err))
			}
			defer wg.Done()
		}(srcdb)

	}
	wg.Wait()
	return nil
}

// migrate one database of a data source
func MigrateDatabase(srcdb *srcreader.SrcDatabase, db *sql.DB) error {
	println("======= migrate database [" + srcdb.Name + "] from " + srcdb.SrcName)
	for _, table := range srcdb.Tables {
		if err := MigrateTable(srcdb, table, db); err != nil {
			return fmt.Errorf("error while migrating table [%s]:\n%s", table, err)
		}
	}
	return nil
}

func generateBatchInsertStmts(dbname string, tablename string, columnNames []string, batchSize int) string {
	var str strings.Builder
	valuesString := fmt.Sprintf("(?%s)", strings.Repeat(",?", len(columnNames)-1))
	str.WriteString(fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES %s", dbname, tablename, valuesString))
	for i := 0; i < batchSize-1; i++ {
		str.WriteRune(',')
		str.WriteString(valuesString)
	}
	str.WriteString(" ON DUPLICATE KEY UPDATE ")
	for i, colName := range columnNames {
		str.WriteString(fmt.Sprintf("`%s`=IF(`updated_at`>VALUES(`updated_at`),`%s`,VALUES(`%s`))", colName, colName, colName))
		if i != len(columnNames)-1 {
			str.WriteRune(',')
		}
	}
	str.WriteString(";")
	return str.String()
}

// migrate one table from a source database
func MigrateTable(srcdb *srcreader.SrcDatabase, tablename string, db *sql.DB) error {
	println("* migrate table " + tablename + " from database " + srcdb.Name + " from " + srcdb.SrcName)

	// create the database and table by importing .sql file
	sql, err := srcdb.ReadSQL(tablename)
	if err != nil {
		return err
	}

	tx0, err := db.Begin()
	if err != nil {
		return errors.New("failed creating transaction tx0: " + err.Error())
	}

	prepStmts := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", srcdb.Name),
		fmt.Sprintf("USE `%s`;", srcdb.Name),
		string(sql),
	}

	for _, stmt := range prepStmts {
		_, err = tx0.Exec(stmt)
		if err != nil {
			return errors.New("failed creating database and table: executing\n" + stmt + "\nerror:" + err.Error())
		}
	}

	// detect the schema of the table
	rows, err := db.Query("SELECT `COLUMN_NAME` FROM information_schema.`COLUMNS` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY `ORDINAL_POSITION`;", srcdb.Name, tablename)
	if err != nil {
		return fmt.Errorf("failed reading schema of %s.%s: %s", srcdb.Name, tablename, err.Error())
	}

	var columnNames []string

	var columnCount int
	for rows.Next() {
		var columnName string
		rows.Scan(&columnName)
		columnNames = append(columnNames, columnName)
		columnCount++
	}
	rows.Close()

	fmt.Printf("columns of %s.%s: %v\n", srcdb.Name, tablename, columnNames)

	// try to resume from a previous migration
	fmt.Printf("reading migration log for source %s db %s table %s\n", srcdb.SrcName, srcdb.Name, tablename)
	rows, err = db.Query("SELECT `seek` FROM meta_migration.migration_log WHERE dbname = ? AND tablename = ? AND src = ?;", srcdb.Name, tablename, srcdb.SrcName)

	if err != nil {
		return errors.New("failed reading migration log: " + err.Error())
	}

	var seek int = -2

	for rows.Next() {
		err := rows.Scan(&seek)
		if err != nil {
			return err
		}
		fmt.Printf("* source %s db %s table %s resuming from seek %d\n", srcdb.SrcName, srcdb.Name, tablename, seek)
	}
	rows.Close()

	if seek == -1 {
		fmt.Printf("* source %s db %s table %s already finished.\n", srcdb.SrcName, srcdb.Name, tablename)
		return nil
	}

	totalTableRowCount := 0
	totalLines := 0
	isResumed := true

	if seek == -2 { // first time migrating the table
		seek = 0
		isResumed = false
		fmt.Printf("* source %s db %s table %s fresh from seek %d\n", srcdb.SrcName, srcdb.Name, tablename, seek)
		// create migration log & potentially create temp primary key
		// primary key detection
		/*
			> 如果有主键或者非空唯一索引，唯一索引相同的情况下，以行updated_at时间戳来判断是否覆盖数据，如果updated_at比原来的数据更新，那么覆盖数据；否则忽略数据。不存在主键相同，updated_at时间戳相同，但数据不同的情况。
			> 如果没有主键或者非空唯一索引，如果除updated_at其他数据都一样，只更新updated_at字段；否则，插入一条新的数据。
			第二种情况，通过添加一个临时主键，转换为第一种。
		*/
		tx0, err := db.Begin()
		if err != nil {
			return errors.New("failed creating tx0 when creating migration log: " + err.Error())
		}

		indres, err := tx0.Query(fmt.Sprintf("SHOW INDEXES IN `%s`.`%s`;", srcdb.Name, tablename))
		if err != nil {
			return errors.New("failed reading primary key: " + err.Error())
		}
		hasIndex := false
		for indres.Next() {
			hasIndex = true
		}
		indres.Close()
		if !hasIndex { // add a temporary primary key for deduplication if no pre-existing primary key was found
			fmt.Printf("* `%s`.`%s` doesn't have a primary key, creating one (id, a, b) for deduplication purposes\n", srcdb.Name, tablename)
			_, err = tx0.Query(fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD PRIMARY KEY (id, a, b);", srcdb.Name, tablename))
			if err != nil {
				return errors.New("failed adding temp primary key: " + err.Error())
			}
		}
		if err != nil {
			return errors.New("failed creating trasaction for creating migration log: " + err.Error())
		}
		_, err = tx0.Query("INSERT INTO meta_migration.migration_log VALUES(?, ?, ?, 0, ?) ON DUPLICATE KEY UPDATE seek = 0;", srcdb.Name, tablename, srcdb.SrcName, !hasIndex)
		if err != nil {
			return errors.New("failed creating migration log: " + err.Error())
		}

		// commit change to migration log

		err = tx0.Commit()
		if err != nil {
			return errors.New("failed committing tx0 when creating migration log: " + err.Error())
		}
	}

	csv, err := srcdb.OpenCSV(tablename, int64(seek))
	if err != nil {
		return err
	}

	lastSeek := seek

	// batch insert
	for {
		batchStartTime := time.Now()
		// create a transaction for this batch of data
		tx, err := db.Begin()
		if err != nil {
			return errors.New("failed creating transaction: " + err.Error())
		}

		// generate the batch insert sql statement

		stmt, err := tx.Prepare(generateBatchInsertStmts(srcdb.Name, tablename, columnNames, BatchSize))
		if err != nil {
			return errors.New("failed preparing insert statement: " + err.Error())
		}

		var batchData []interface{}
		for rowCount := 0; rowCount < BatchSize; rowCount++ {
			line, err := csv.ReadBytes('\n')
			if err == io.EOF {
				// table finished, part of the last batch

				// prepare a shorter batch insert statement just for the last batch
				stmt, err = tx.Prepare(generateBatchInsertStmts(srcdb.Name, tablename, columnNames, rowCount))
				if err != nil {
					return errors.New("failed preparing insert statement: " + err.Error())
				}

				seek = -1
				break
			}
			if err != nil {
				return fmt.Errorf("failed reading csv from seek pos %d: %s", seek, err.Error())
			}
			totalLines++
			data := strings.Split(string(line), ",")
			batchData = append(batchData, data[0])
			batchData = append(batchData, data[1])
			batchData = append(batchData, data[2])
			batchData = append(batchData, data[3])
			seek += len(line)
		}

		res, err := stmt.Exec(batchData...) // insert one batch of data
		if err != nil {
			return fmt.Errorf("failed exec batch seek %d source %s db %s table %s: %s", seek, srcdb.SrcName, srcdb.Name, tablename, err.Error())
		}

		// update migration log at the end of the transaction
		_, err = tx.Query("UPDATE meta_migration.migration_log SET seek = ? WHERE dbname = ? AND tablename = ? AND src = ?;", seek, srcdb.Name, tablename, srcdb.SrcName)
		if err != nil {
			return fmt.Errorf("failed updating migration log for source %s db %s table %s, new seek = %d: %s", srcdb.SrcName, srcdb.Name, tablename, seek, err.Error())
		}

		err = tx.Commit()
		if err != nil {
			return errors.New("failed commiting transaction: " + err.Error())
		}

		rowsAffected, _ := res.RowsAffected()

		speed := float32(seek-lastSeek) / float32(time.Since(batchStartTime).Milliseconds()) * 1000 / 1024
		fmt.Printf("batchok %s %s.%s, new seek = %d, rows = %d, %.2fKB/s (%.2fs)\n", srcdb.SrcName, srcdb.Name, tablename, seek, rowsAffected, speed, time.Since(batchStartTime).Seconds())

		totalTableRowCount += int(rowsAffected)
		stats.ReportBytesMigrated(seek - lastSeek)

		lastSeek = seek

		if seek == -1 {
			break
		}
	}

	fmt.Printf("* finished table source %s db %s table %s, totalRowAffected %d, csvlines: %d (resumed: %v)\n", srcdb.SrcName, srcdb.Name, tablename, totalTableRowCount, totalLines, isResumed)

	return nil
}

// run after all databases and tables from all sources are fully migrated
func PostJob(db *sql.DB) error {
	println("* postjob started")

	tx0, err := db.Begin()
	if err != nil {
		return errors.New("postjob failed creating transaction tx0: " + err.Error())
	}

	res, err := tx0.Query("SELECT dbname, tablename FROM meta_migration.migration_log WHERE `temp_prikey` = 1 GROUP BY dbname, tablename;")
	if err != nil {
		return errors.New("postjob failed selecting temp_prikey to remove from migration log: " + err.Error())
	}

	var dbnames, tablenames []string
	for res.Next() {
		var dbname, tablename string
		res.Scan(&dbname, &tablename)
		dbnames = append(dbnames, dbname)
		tablenames = append(tablenames, tablename)
	}

	res.Close()

	for i, dbname := range dbnames {
		tablename := tablenames[i]
		fmt.Printf("* removing temp_prikey of %s.%s from migration_log\n", dbname, tablename)
		_, err = db.Query("UPDATE meta_migration.migration_log SET temp_prikey = 0 WHERE dbname = ? AND tablename = ?;", dbname, tablename)
		if err != nil {
			return fmt.Errorf("postjob failed updating migration log for %s.%s after removing temp_prikey: %s", dbname, tablename, err.Error())
		}
	}

	tx0.Commit()

	// DDL statement triggers a implicit commit, so should do it after updating meta_migration
	// HOWEVER, bad thing could happen if the program is stopped right now, and might result in extra primary keys not being deleted.

	fmt.Printf("=== all temp_prikeys has been committed into migration_log\n")
	fmt.Printf("=== start actually dropping primary keys.\n") // if the program stops right now, primary keys might not have been dropped yet.
	// TODO: potential solution: use a log locally to record if each temp primary key has actually been dropped yet.

	for i, dbname := range dbnames {
		tablename := tablenames[i]
		fmt.Printf("* removing temp prikey from %s.%s\n", dbname, tablename)
		_, err = db.Query(fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PRIMARY KEY;", dbname, tablename))
		if err != nil {
			// return fmt.Errorf("postjob failed dropping temp primary key for %s.%s: %s", dbname, tablename, err.Error())
			fmt.Printf("error: postjob failed dropping temp primary key for %s.%s: %s\n", dbname, tablename, err.Error())
			// this error has been temporarily softened so that it would not cause a panic at the very last stage of the operation
		}
	}

	println("* postjob finished")
	return nil
}
