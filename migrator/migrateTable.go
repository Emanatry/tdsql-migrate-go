package migrator

import (
	"bufio"
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Emanatry/tdsql-migrate-go/srcreader"
	"github.com/Emanatry/tdsql-migrate-go/stats"
)

func generateBatchInsertStmts(dbname string, tablename string, columnNames []string, batchSize int) string {
	var str strings.Builder
	valuesString := fmt.Sprintf("(?%s)", strings.Repeat(",?", len(columnNames)-1))
	str.WriteString(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s", dbname, tablename, strings.Join(columnNames, ","), valuesString))
	for i := 0; i < batchSize-1; i++ {
		str.WriteRune(',')
		str.WriteString(valuesString)
	}
	str.WriteString(" ON DUPLICATE KEY UPDATE updated_at=updated_at") // ignore rows with duplicate key

	return str.String()
}

const keyIdBString = ",\n  KEY (`id`,`b`)"

func createTable(srcdb *srcreader.SrcDatabase, tablename string, db *sql.DB) error {
	// create the database and table by importing .sqlfile file
	sqlfile, err := srcdb.ReadSQL(tablename)
	if err != nil {
		return err
	}

	tx0, err := db.Begin()
	if err != nil {
		return errors.New("failed creating transaction tx0: " + err.Error())
	}

	// for better performance in table 4, create the key after the migration finishes
	// dirty hack to remove index from table 4
	if bytes.Contains(sqlfile, []byte(keyIdBString)) {
		fmt.Printf("* temporarilySuppressKeyIdB: %s.%s\n", srcdb.Name, tablename)
		sqlfile = bytes.Replace(sqlfile, []byte(keyIdBString), []byte{}, -1)
	}
	// another dirty hack to add shard key (tdsql only)
	if !bytes.Contains(sqlfile, []byte("PRIMARY KEY")) { // must have primary key to use shard key
		fmt.Printf("* adding primary key(id,a,b) to %s.%s\n", srcdb.Name, tablename)
		idx := bytes.Index(sqlfile, []byte(") ENGINE=InnoDB DEFAULT CHARSET=utf8"))
		sqlfile = bytes.Join([][]byte{sqlfile[:idx], []byte(",\n  PRIMARY KEY(`id`,`a`,`b`)\n"), sqlfile[idx:]}, []byte{})
	}
	sqlfile = append(sqlfile, " shardkey=id"...)

	prepStmts := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", srcdb.Name),
		fmt.Sprintf("USE `%s`;", srcdb.Name),
		string(sqlfile),
	}

	fmt.Printf("=== %s %s.%s's table creation sql(after transformation):\n%s\n=== end table creation sql\n\n", srcdb.SrcName, srcdb.Name, tablename, sqlfile)

	for _, stmt := range prepStmts {
		_, err = tx0.Exec(stmt)
		if err != nil {
			return errors.New("failed creating database and table: executing\n" + stmt + "\nerror:" + err.Error())
		}
	}

	tx0.Commit()
	return nil
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

func migrationStepInitMigrationLog(srcdba *srcreader.SrcDatabase, srcdbb *srcreader.SrcDatabase, tablename string, db *sql.DB, columnNames []string) error {
	fmt.Printf("* fresh start %s %s.%s from seek %d\n", srcdba.SrcName, srcdba.Name, tablename, 0)
	// create migration log & potentially create temp primary key

	err := writeSeekMigrationLog(srcdba.SrcName, srcdba.Name, tablename, 0)
	if err != nil {
		return errors.New("failed creating migration log: " + err.Error())
	}

	return nil
}

// migrate one table from a source database
// nodup: true if the data source has already been deduped and there's no need to do that while migrating.
func MigrateTable(srcdba *srcreader.SrcDatabase, srcdbb *srcreader.SrcDatabase, tablename string, db *sql.DB) error {
	println("* migrate table " + tablename + " from database " + srcdba.Name)

	var err error

	sqlfile, err := srcdba.ReadSQL(tablename)
	if err != nil {
		return err
	}

	var temporarilySuppressKeyIdB = false
	// dirty hack to remove index from table 4
	if bytes.Contains(sqlfile, []byte(keyIdBString)) {
		fmt.Printf("* !temporarilySuppressKeyIdB: %s.%s\n", srcdba.Name, tablename)
		temporarilySuppressKeyIdB = true
	}

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

	columnNames, err := migrationStepDetectColumns(srcdba, srcdbb, tablename, db)
	if err != nil {
		return err
	}

	seek, err := readSeekMigrationLog(srcdba.SrcName, srcdba.Name, tablename)
	if err != nil {
		return err
	}
	if seek >= 0 {
		fmt.Printf("* resuming %s %s.%s from seek %d\n", srcdba.SrcName, srcdba.Name, tablename, seek)
	} else if seek == -1 {
		fmt.Printf("* %s %s.%s already finished.\n", srcdba.SrcName, srcdba.Name, tablename)
		return nil
	}

	totalTableRowCount := 0
	totalLines := 0
	isResumed := true

	if seek == -2 { // first time migrating the table
		seek = 0
		isResumed = false
		err := migrationStepInitMigrationLog(srcdba, srcdbb, tablename, db, columnNames)
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
	fullBatchInsertSqlStmtsStr := generateBatchInsertStmts(srcdba.Name, tablename, columnNames, BATCH_SIZE)
	fullBatchInsertSqlStmts, err := db.Prepare(fullBatchInsertSqlStmtsStr)
	if err != nil {
		return errors.New("failed preparing insert statement: " + err.Error())
	}

	batchCounter := 0

	tx0, err := db.Begin()
	if err != nil {
		return errors.New("failed creating initial tx0 for batch insert: " + err.Error())
	}
	txstmt := tx0.Stmt(fullBatchInsertSqlStmts)

	// batch insert
	for {
		batchStartTime := time.Now()
		// create a transaction for this batch of data

		// generate the batch insert sql statement
		var batchData []interface{}
		for rowCount := 0; rowCount < BATCH_SIZE; rowCount++ {
			line, err := csv.ReadBytes('\n')
			if err == io.EOF {
				// table finished, part of the last batch

				// prepare a shorter batch insert statement just for the last batch
				txstmt, err = tx0.Prepare(generateBatchInsertStmts(srcdba.Name, tablename, columnNames, rowCount))
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
			data := strings.Split(strings.TrimSpace(string(line)), ",")
			for i := 0; i < len(columnNames); i++ {
				// convert input data into their corresponding native types
				var converted interface{}
				var err error
				switch i {
				case 0: // bigint unsigned
					converted, err = strconv.ParseUint(data[i], 10, 64)
				case 1: // float/double
					// always treat it as 64-bit, despite having both float and double as input data.
					converted, err = strconv.ParseFloat(data[i], 64)
				case 2: // char(32)
					// it actually costs more to transmit binary and UNHEX it at the other end
					// so here we just transmit the raw string data
					converted, err = data[i], nil
				case 3: // datetime
					converted, err = time.ParseInLocation("2006-01-02 15:04:05", data[i], time.Local)
				}
				if err != nil {
					return fmt.Errorf("failed converting input data [%s]: %s", data[i], err)
				}
				// fmt.Printf("[%+v]\n", converted)
				batchData = append(batchData, converted)
			}
			seek += len(line)
		}

		res, err := txstmt.Exec(batchData...) // insert one batch of data
		if err != nil {
			return fmt.Errorf("failed exec batch seek %d source %s %s.%s: %s", seek, srcdba.SrcName, srcdba.Name, tablename, err.Error())
		}

		batchCounter++
		if batchCounter >= COMMIT_INTERVAL {
			batchCounter = 0
			err := tx0.Commit()
			if err != nil {
				return fmt.Errorf("failed committing tx0 for %s.%s: %s", srcdba.Name, tablename, err.Error())
			}
			fmt.Printf("committing %s.%s\n", srcdba.Name, tablename)
			err = writeSeekMigrationLog(srcdba.SrcName, srcdba.Name, tablename, seek)
			if err != nil {
				return fmt.Errorf("failed updating migration log for source %s %s.%s, new seek = %d: %s", srcdba.SrcName, srcdba.Name, tablename, seek, err.Error())
			}

			tx0, err = db.Begin()
			if err != nil {
				return errors.New("failed creating new tx0 for batch insert: " + err.Error())
			}
			txstmt = tx0.Stmt(fullBatchInsertSqlStmts)
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
				fmt.Printf("* adding back key id_b for %s.%s\n", srcdba.Name, tablename)
				t1 := time.Now()
				var err error = nil
				for err == nil || strings.Contains(err.Error(), "Lock wait timeout exceeded") {
					_, err = db.Exec(fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD INDEX (`id`,`b`);", srcdba.Name, tablename))
					if err == nil {
						break
					}
					fmt.Println("retry......")
				}
				if err != nil {
					return errors.New("failed adding back KEY(`id`,`b`): " + err.Error())
				}
				fmt.Printf("* rebuilt key id_b for %s.%s in %.1f secs.\n", srcdba.Name, tablename, time.Since(t1).Seconds())
			}
			break
		}
	}

	err = tx0.Commit()
	if err != nil {
		return fmt.Errorf("failed committing last tx0 for %s.%s: %s", srcdba.Name, tablename, err.Error())
	}

	fullBatchInsertSqlStmts.Close()

	fmt.Printf("* finished table db %s table %s, totalRowAffected %d, csvlines: %d (resumed: %v)\n", srcdba.Name, tablename, totalTableRowCount, totalLines, isResumed)

	return nil
}
