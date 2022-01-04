package migrator

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Emanatry/tdsql-migrate-go/srcreader"
)

func checkAndCreatePKForDedup(db *sql.DB, srcdb *srcreader.SrcDatabase, tablename string, columnNames []string) error {
	// primary key detection
	/*
		> 如果有主键或者非空唯一索引，唯一索引相同的情况下，以行updated_at时间戳来判断是否覆盖数据，如果updated_at比原来的数据更新，那么覆盖数据；否则忽略数据。不存在主键相同，updated_at时间戳相同，但数据不同的情况。
		> 如果没有主键或者非空唯一索引，如果除updated_at其他数据都一样，只更新updated_at字段；否则，插入一条新的数据。
		第二种情况，通过添加一个包括所有数据列，但不包括 updated_at 的临时主键，转换为第一种。
	*/
	indres, err := db.Query(fmt.Sprintf("SHOW INDEXES IN `%s`.`%s`;", srcdb.Name, tablename))

	if err != nil {
		return errors.New("failed reading primary key: " + err.Error())
	}
	cols, err := indres.Columns()
	if err != nil {
		panic(err)
	}
	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	var discardedBytes []byte
	var nonUnique bool = true
	var keyName string

	hasUniqueIndex := false

	for i := range dest {
		dest[i] = &discardedBytes
		if cols[i] == "Key_name" {
			dest[i] = &keyName
		} else if cols[i] == "Non_unique" {
			dest[i] = &nonUnique
		}
	}
	for indres.Next() {
		err = indres.Scan(dest...)
		if err != nil {
			return errors.New("failed to scan while showing index: " + err.Error())
		}
		if !nonUnique {
			fmt.Printf("found unique index for %s.%s: %s\n", srcdb.Name, tablename, keyName)
			hasUniqueIndex = true
			break
		}
	}
	indres.Close()

	var columnNamesMinusUpdatedAt []string
	for _, v := range columnNames {
		if v != "updated_at" {
			columnNamesMinusUpdatedAt = append(columnNamesMinusUpdatedAt, v)
		}
	}
	keyColumnsStr := strings.Join(columnNamesMinusUpdatedAt, ", ")
	if !hasUniqueIndex { // add a temporary primary key of all columns for deduplication if no pre-existing unique key was found
		fmt.Printf("* %s.%s doesn't have a unique key, creating one (%s) for deduplication purposes\n", srcdb.Name, tablename, keyColumnsStr)
		_, err = db.Exec(fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD PRIMARY KEY (%s);", srcdb.Name, tablename, keyColumnsStr))
		if err != nil {
			return errors.New("failed adding temp primary key: " + err.Error())
		}
	} else {
		fmt.Printf("* %s.%s has unique key.\n", srcdb.Name, tablename)
	}
	return nil
}
