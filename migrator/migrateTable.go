package migrator

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Emanatry/tdsql-migrate-go/srcreader"
	"github.com/Emanatry/tdsql-migrate-go/stats"
)

func generateBatchInsertStmts(dbname string, tablename string, columnNames []string, batchSize int, nodup bool) string {
	var str strings.Builder
	valuesString := fmt.Sprintf("(?%s)", strings.Repeat(",?", len(columnNames)-1))
	str.WriteString(fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES %s", dbname, tablename, valuesString))
	for i := 0; i < batchSize-1; i++ {
		str.WriteRune(',')
		str.WriteString(valuesString)
	}
	if !nodup {
		str.WriteString(" ON DUPLICATE KEY UPDATE ")
		for i, colName := range columnNames {
			str.WriteString(fmt.Sprintf("`%s`=IF(`updated_at`>=VALUES(`updated_at`),`%s`,VALUES(`%s`))", colName, colName, colName))
			if i != len(columnNames)-1 {
				str.WriteRune(',')
			}
		}
	}
	str.WriteString(";")
	return str.String()
}

// migrate one table from a source database
// nodup: true if the data source has already been deduped and there's no need to do that while migrating.
func MigrateTable(srcdb *srcreader.SrcDatabase, tablename string, db *sql.DB, nodup bool) error {
	println("* migrate table " + tablename + " from database " + srcdb.Name + " from " + srcdb.SrcName)

	// create the database and table by importing .sqlfile file
	sqlfile, err := srcdb.ReadSQL(tablename)

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
		string(sqlfile),
	}

	fmt.Printf("=== %s %s.%s's table creation sql:\n%s\n=== end table creation sql\n\n", srcdb.SrcName, srcdb.Name, tablename, sqlfile)

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

	for rows.Next() {
		var columnName string
		rows.Scan(&columnName)
		columnNames = append(columnNames, columnName)
	}
	rows.Close()

	fmt.Printf("columns of %s.%s: %v\n", srcdb.Name, tablename, columnNames)

	// try to resume from a previous migration
	fmt.Printf("reading migration log for %s %s.%s\n", srcdb.SrcName, srcdb.Name, tablename)
	rows, err = db.Query("SELECT `seek` FROM meta_migration.migration_log WHERE dbname = ? AND tablename = ? AND src = ?;", srcdb.Name, tablename, srcdb.SrcName)

	if err != nil {
		return errors.New("failed reading migration log: " + err.Error())
	}

	if !nodup { // if source data hasn't been deduped locally
		/*
			> 如果有主键或者非空唯一索引，唯一索引相同的情况下，以行updated_at时间戳来判断是否覆盖数据，如果updated_at比原来的数据更新，那么覆盖数据；否则忽略数据。不存在主键相同，updated_at时间戳相同，但数据不同的情况。
			> 如果没有主键或者非空唯一索引，如果除updated_at其他数据都一样，只更新updated_at字段；否则，插入一条新的数据。
			第二种情况，通过添加一个包括所有数据列，但不包括 updated_at 的临时主键，转换为第一种。
		*/
		checkAndCreatePKForDedup(tx0, srcdb, tablename, columnNames)
	}

	var seek int = -2

	for rows.Next() {
		err := rows.Scan(&seek)
		if err != nil {
			return err
		}
		fmt.Printf("* resuming %s %s.%s from seek %d\n", srcdb.SrcName, srcdb.Name, tablename, seek)
	}
	rows.Close()

	if seek == -1 {
		fmt.Printf("* %s %s.%s already finished.\n", srcdb.SrcName, srcdb.Name, tablename)
		return nil
	}

	totalTableRowCount := 0
	totalLines := 0
	isResumed := true

	if seek == -2 { // first time migrating the table
		seek = 0
		isResumed = false
		fmt.Printf("* fresh start %s %s.%s from seek %d\n", srcdb.SrcName, srcdb.Name, tablename, seek)
		// create migration log & potentially create temp primary key

		tx0, err := db.Begin()
		if err != nil {
			return errors.New("failed creating tx0 when creating migration log: " + err.Error())
		}

		_, err = tx0.Exec("INSERT INTO meta_migration.migration_log VALUES(?, ?, ?, 0, ?) ON DUPLICATE KEY UPDATE seek = 0;", srcdb.Name, tablename, srcdb.SrcName, 0 /* !hasUniqueIndex */)
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

	// sql statement in string form for a full BatchSize batch insert.
	fullBatchInsertSqlStmtsStr := generateBatchInsertStmts(srcdb.Name, tablename, columnNames, BatchSize, nodup)

	// batch insert
	for {
		batchStartTime := time.Now()
		// create a transaction for this batch of data
		tx, err := db.Begin()
		if err != nil {
			return errors.New("failed creating transaction: " + err.Error())
		}

		// generate the batch insert sql statement
		var stmt *sql.Stmt
		isFullBatch := true
		var batchData []interface{}
		for rowCount := 0; rowCount < BatchSize; rowCount++ {
			line, err := csv.ReadBytes('\n')
			if err == io.EOF {
				// table finished, part of the last batch

				// prepare a shorter batch insert statement just for the last batch
				stmt, err = tx.Prepare(generateBatchInsertStmts(srcdb.Name, tablename, columnNames, rowCount, nodup))
				if err != nil {
					return errors.New("failed preparing insert statement: " + err.Error())
				}

				isFullBatch = false
				seek = -1
				break
			}
			if err != nil {
				return fmt.Errorf("failed reading csv from seek pos %d: %s", seek, err.Error())
			}
			totalLines++
			data := strings.Split(string(line), ",")
			for i := 0; i < len(columnNames); i++ {
				batchData = append(batchData, data[i])
			}
			seek += len(line)
		}

		if isFullBatch {
			stmt, err = tx.Prepare(fullBatchInsertSqlStmtsStr)
			if err != nil {
				return errors.New("failed preparing insert statement: " + err.Error())
			}
		}

		res, err := stmt.Exec(batchData...) // insert one batch of data
		if err != nil {
			return fmt.Errorf("failed exec batch seek %d source %s %s.%s: %s", seek, srcdb.SrcName, srcdb.Name, tablename, err.Error())
		}
		stmt.Close()

		// update migration log at the end of the transaction
		_, err = tx.Exec("UPDATE meta_migration.migration_log SET seek = ? WHERE dbname = ? AND tablename = ? AND src = ?;", seek, srcdb.Name, tablename, srcdb.SrcName)
		if err != nil {
			return fmt.Errorf("failed updating migration log for source %s %s.%s, new seek = %d: %s", srcdb.SrcName, srcdb.Name, tablename, seek, err.Error())
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