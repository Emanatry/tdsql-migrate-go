package migrator

import (
	"bufio"
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Emanatry/tdsql-migrate-go/srcreader"
	"github.com/Emanatry/tdsql-migrate-go/stats"
)

func generateBatchInsertStmts(dbname string, tablename string, columnNames []string, batchSize int, nodup bool) string {
	var str strings.Builder
	valuesString := fmt.Sprintf("(?%s)", strings.Repeat(",?", len(columnNames)-1))
	str.WriteString(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s", dbname, tablename, strings.Join(columnNames, ","), valuesString))
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
	return str.String()
}

func migrationStepCreateTable(srcdba *srcreader.SrcDatabase, srcdbb *srcreader.SrcDatabase, tablename string, db *sql.DB) (temporarilySuppressKeyIdB bool, err error) {
	// create the database and table by importing .sqlfile file
	sqlfile, err := srcdba.ReadSQL(tablename)
	if err != nil {
		return false, err
	}

	tx0, err := db.Begin()
	if err != nil {
		return false, errors.New("failed creating transaction tx0: " + err.Error())
	}

	const keyIdBString = ",\n  KEY (`id`,`b`)"
	temporarilySuppressKeyIdB = false // for better performance in table 4, create the key after the migration finishes
	// dirty hack to remove index from table 4
	if bytes.Contains(sqlfile, []byte(keyIdBString)) {
		fmt.Printf("* temporarilySuppressKeyIdB: %s.%s\n", srcdba.Name, tablename)
		temporarilySuppressKeyIdB = true
		sqlfile = bytes.Replace(sqlfile, []byte(keyIdBString), []byte{}, -1)
	}
	// another dirty hack to add shard key (tdsql only)
	if !bytes.Contains(sqlfile, []byte("PRIMARY KEY")) { // must have primary key to use shard key
		fmt.Printf("* adding primary key(id,a,b) to %s.%s\n", srcdba.Name, tablename)
		idx := bytes.Index(sqlfile, []byte(") ENGINE=InnoDB DEFAULT CHARSET=utf8"))
		sqlfile = bytes.Join([][]byte{sqlfile[:idx], []byte(",\n  PRIMARY KEY(`id`,`a`,`b`)\n"), sqlfile[idx:]}, []byte{})
	}
	sqlfile = append(sqlfile, " shardkey=id"...)

	prepStmts := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", srcdba.Name),
		fmt.Sprintf("USE `%s`;", srcdba.Name),
		string(sqlfile),
	}

	fmt.Printf("=== %s %s.%s's table creation sql(after transformation):\n%s\n=== end table creation sql\n\n", srcdba.SrcName, srcdba.Name, tablename, sqlfile)

	for _, stmt := range prepStmts {
		_, err = tx0.Exec(stmt)
		if err != nil {
			return false, errors.New("failed creating database and table: executing\n" + stmt + "\nerror:" + err.Error())
		}
	}
	return temporarilySuppressKeyIdB, nil
}

func migrationStepDetectColumns(srcdba *srcreader.SrcDatabase, srcdbb *srcreader.SrcDatabase, tablename string, db *sql.DB) ([]string, error) {

	// detect the schema of the table
	rows, err := db.Query("SELECT `COLUMN_NAME` FROM information_schema.`COLUMNS` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY `ORDINAL_POSITION`;", srcdba.Name, tablename)
	if err != nil {
		return nil, fmt.Errorf("failed reading schema of %s.%s: %s", srcdba.Name, tablename, err.Error())
	}

	var columnNames []string

	for rows.Next() {
		var columnName string
		rows.Scan(&columnName)
		columnNames = append(columnNames, columnName)
	}
	rows.Close()

	fmt.Printf("columns of %s.%s: %v\n", srcdba.Name, tablename, columnNames)
	return columnNames, nil
}

// return value: -3: error, -2: not started, -1: finished, any other non-negative number: continue from this position
func migrationStepReadMigrationLog(srcdba *srcreader.SrcDatabase, srcdbb *srcreader.SrcDatabase, tablename string, db *sql.DB) (seek int, err error) {
	// try to resume from a previous migration
	fmt.Printf("reading migration log for %s %s.%s\n", srcdba.SrcName, srcdba.Name, tablename)
	rows, err := db.Query("SELECT `seek` FROM meta_migration.migration_log WHERE dbname = ? AND tablename = ? AND src = ?;", srcdba.Name, tablename, srcdba.SrcName)

	if err != nil {
		return -3, errors.New("failed reading migration log: " + err.Error())
	}

	seek = -2

	for rows.Next() {
		err := rows.Scan(&seek)
		if err != nil {
			return -3, err
		}
		fmt.Printf("* resuming %s %s.%s from seek %d\n", srcdba.SrcName, srcdba.Name, tablename, seek)
	}
	rows.Close()
	return seek, nil
}

func migrationStepInitMigrationLog(srcdba *srcreader.SrcDatabase, srcdbb *srcreader.SrcDatabase, tablename string, db *sql.DB, columnNames []string, nodup bool) error {
	fmt.Printf("* fresh start %s %s.%s from seek %d\n", srcdba.SrcName, srcdba.Name, tablename, 0)
	// create migration log & potentially create temp primary key

	tx0, err := db.Begin()
	if err != nil {
		return errors.New("failed creating tx0 when creating migration log: " + err.Error())
	}

	_, err = tx0.Exec("INSERT INTO meta_migration.migration_log (dbname,tablename,src,seek,temp_prikey) VALUES(?, ?, ?, 0, ?) ON DUPLICATE KEY UPDATE seek = 0;", srcdba.Name, tablename, srcdba.SrcName, 0 /* !hasUniqueIndex */)
	if err != nil {
		return errors.New("failed creating migration log: " + err.Error())
	}

	if !nodup { // if source data hasn't been deduped locally
		/*
			> 如果有主键或者非空唯一索引，唯一索引相同的情况下，以行updated_at时间戳来判断是否覆盖数据，如果updated_at比原来的数据更新，那么覆盖数据；否则忽略数据。不存在主键相同，updated_at时间戳相同，但数据不同的情况。
			> 如果没有主键或者非空唯一索引，如果除updated_at其他数据都一样，只更新updated_at字段；否则，插入一条新的数据。
			第二种情况，通过添加一个包括所有数据列，但不包括 updated_at 的临时主键，转换为第一种。
		*/
		checkAndCreatePKForDedup(tx0, srcdba, tablename, columnNames)
	}

	// commit change to migration log

	err = tx0.Commit()
	if err != nil {
		return errors.New("failed committing tx0 when creating migration log: " + err.Error())
	}
	return nil
}

// migrate one table from a source database
// nodup: true if the data source has already been deduped and there's no need to do that while migrating.
func MigrateTable(srcdba *srcreader.SrcDatabase, srcdbb *srcreader.SrcDatabase, tablename string, db *sql.DB, nodup bool) error {
	println("* migrate table " + tablename + " from database " + srcdba.Name)

	var err error

	/// ======= preparation =======

	// sort and merge the two tables from source a and b
	err = srcdba.PresortTable(tablename)
	if err != nil {
		return err
	}
	err = srcdbb.PresortTable(tablename)
	if err != nil {
		return err
	}
	mergedCsvPath, err := srcreader.MergeSortedTable(srcdba, srcdbb, tablename)
	if err != nil {
		return err
	}

	temporarilySuppressKeyIdB, err := migrationStepCreateTable(srcdba, srcdbb, tablename, db)
	if err != nil {
		return err
	}

	columnNames, err := migrationStepDetectColumns(srcdba, srcdbb, tablename, db)
	if err != nil {
		return err
	}

	seek, err := migrationStepReadMigrationLog(srcdba, srcdbb, tablename, db)
	if err != nil {
		return err
	}

	if seek == -1 {
		fmt.Printf("* %s %s.%s already finished.\n", srcdba.SrcName, srcdba.Name, tablename)
		return nil
	}

	totalTableRowCount := 0
	totalLines := 0
	isResumed := true

	if seek == -2 { // first time migrating the table
		seek = 0
		isResumed = false
		err := migrationStepInitMigrationLog(srcdba, srcdbb, tablename, db, columnNames, nodup)
		if err != nil {
			return err
		}
	}

	/// ======= migration =======

	csvfile, err := os.Open(mergedCsvPath)
	if err != nil {
		return err
	}
	if seek != 0 {
		csvfile.Seek(int64(seek), 0)
	}

	fileinfo, err := csvfile.Stat()
	if err != nil {
		return err
	}
	csvsize := fileinfo.Size()

	csv := bufio.NewReader(csvfile)

	lastSeek := seek

	// sql statement in string form for a full BatchSize batch insert.
	fullBatchInsertSqlStmtsStr := generateBatchInsertStmts(srcdba.Name, tablename, columnNames, BatchSize, nodup)
	fullBatchInsertSqlStmts, err := db.Prepare(fullBatchInsertSqlStmtsStr)
	if err != nil {
		return errors.New("failed preparing insert statement: " + err.Error())
	}

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
				stmt, err = tx.Prepare(generateBatchInsertStmts(srcdba.Name, tablename, columnNames, rowCount, nodup))
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
			stmt = tx.Stmt(fullBatchInsertSqlStmts)
			if err != nil {
				return errors.New("failed preparing insert statement: " + err.Error())
			}
		}

		res, err := stmt.Exec(batchData...) // insert one batch of data
		if err != nil {
			return fmt.Errorf("failed exec batch seek %d source %s %s.%s: %s", seek, srcdba.SrcName, srcdba.Name, tablename, err.Error())
		}
		stmt.Close()

		// update migration log at the end of the transaction
		_, err = tx.Exec("UPDATE meta_migration.migration_log SET seek = ? WHERE dbname = ? AND tablename = ? AND src = ?;", seek, srcdba.Name, tablename, srcdba.SrcName)
		if err != nil {
			return fmt.Errorf("failed updating migration log for source %s %s.%s, new seek = %d: %s", srcdba.SrcName, srcdba.Name, tablename, seek, err.Error())
		}

		err = tx.Commit()
		if err != nil {
			return errors.New("failed commiting transaction: " + err.Error())
		}

		rowsAffected, _ := res.RowsAffected()

		if !stats.DevSuppressLog {
			speed := float32(seek-lastSeek) / float32(time.Since(batchStartTime).Milliseconds()) * 1000 / 1024
			fmt.Printf("batchok %s %s.%s, new seek = (%.2f%%) %d, rows = %d, %.2fKB/s (%.2fs)\n", srcdba.SrcName, srcdba.Name, tablename, float64(seek)/float64(csvsize)*100, seek, rowsAffected, speed, time.Since(batchStartTime).Seconds())
		}

		totalTableRowCount += int(rowsAffected)
		stats.ReportBytesMigrated(seek - lastSeek)

		lastSeek = seek

		if seek == -1 {
			if temporarilySuppressKeyIdB { // add back KEY(`id`,`b`)
				fmt.Printf("* adding back temporarilySuppressKeyIdB for %s.%s\n", srcdba.Name, tablename)
				t1 := time.Now()
				_, err = db.Exec(fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD INDEX (`id`,`b`);", srcdba.Name, tablename))
				if err != nil {
					return errors.New("failed adding back KEY(`id`,`b`): " + err.Error())
				}
				fmt.Printf("* rebuilt key id_b for %s.%s in %.1f secs.\n", srcdba.Name, tablename, time.Since(t1).Seconds())
			}
			break
		}
	}

	fmt.Printf("* finished table db %s table %s, totalRowAffected %d, csvlines: %d (resumed: %v)\n", srcdba.Name, tablename, totalTableRowCount, totalLines, isResumed)

	return nil
}
