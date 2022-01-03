package srcreader

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
)

const PRESORT_PATH = "./presort/data/"
const SORTER_PROGRAM = "./presort/sortdata"
const MERGER_PROGRAM = "./presort/merge"
const CONCURRENT_PRESORT_JOB = 3
const CONCURRENT_MERGE_JOB = 3

func (d *SrcDatabase) determinePKColumnType(table string) (string, error) {
	sql, err := d.ReadSQL(table)
	if err != nil {
		return "", err
	}
	sqlstr := string(sql)
	// dirty but works
	if strings.Contains(sqlstr, "PRIMARY KEY (`id`)") {
		return "id", nil
	}
	if strings.Contains(sqlstr, "PRIMARY KEY (`id`,`a`)") {
		return "id_a", nil
	}
	if strings.Contains(sqlstr, "KEY (`id`,`b`)") {
		return "id_b_a", nil
	}
	return "id_a_b", nil
}

func (db *SrcDatabase) getPresortMarkFile(table string) string {
	return PRESORT_PATH + db.SrcName + "/" + db.Name + "/" + table + ".presorted"
}

func (db *SrcDatabase) getPresortOutputFile(table string) string {
	return PRESORT_PATH + db.SrcName + "/" + db.Name + "/" + table + ".csv"
}

func (d *SrcDatabase) IsTablePresorted(table string) bool {
	for i, tb := range d.Tables {
		if table == tb {
			return d.tablePresorted[i]
		}
	}
	return false
}

func (db *SrcDatabase) getTableIndex(table string) int {
	for i, v := range db.Tables {
		if v == table {
			return i
		}
	}
	return -1
}

func (db *SrcDatabase) PresortTable(table string) error {
	err := os.MkdirAll(PRESORT_PATH+db.SrcName+"/"+db.Name, 0755)
	if err != nil {
		return err
	}
	if db.tablePresorted[db.getTableIndex(table)] {
		return nil
	}

	coltype, err := db.determinePKColumnType(table)
	if err != nil {
		return err
	}
	fmt.Printf("@ presorting %s %s.%s (%s)\n", db.SrcName, db.Name, table, coltype)
	// presort files by running c++ sorting program
	cmd := exec.Command(SORTER_PROGRAM, db.srcdbpath+"/"+table+".csv", db.getPresortOutputFile(table), coltype)
	err = cmd.Run()
	if err != nil {
		return err
	}

	// create mark file for presorted tables
	f, err := os.Create(db.getPresortMarkFile(table))
	if err != nil {
		return err
	}
	f.Close()

	db.tablePresorted[db.getTableIndex(table)] = true
	return nil
}

func MergeSortedTable(dba *SrcDatabase, dbb *SrcDatabase, table string) (csvpath string, err error) {
	dbroot := PRESORT_PATH + "merged" + "/" + dba.Name
	mergeOutputFile := dbroot + "/" + table + ".csv"
	err = os.MkdirAll(dbroot, 0755)
	if err != nil {
		return "", err
	}
	markfile := dbroot + "/" + table + ".mark"
	if doFileExists(markfile) {
		return mergeOutputFile, err
	}

	coltype, err := dba.determinePKColumnType(table)
	if err != nil {
		return "", err
	}
	fmt.Printf("@ merging %s.%s (%s)\n", dba.Name, table, coltype)
	sql, err := dba.ReadSQL(table)
	if err != nil {
		return "", err
	}
	err = ioutil.WriteFile(dbroot+"/"+table+".sql", sql, 0755)
	if err != nil {
		return "", err
	}
	cmd := exec.Command(MERGER_PROGRAM, dba.getPresortOutputFile(table), dbb.getPresortOutputFile(table), mergeOutputFile, coltype)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	err = cmd.Start()
	if err != nil {
		return "", err
	}
	out, err := ioutil.ReadAll(stdout)
	if err != nil {
		return "", err
	}
	println(string(out))
	cmd.Wait()

	// create mark file for merged tables
	f, err := os.Create(markfile)
	if err != nil {
		return "", err
	}
	f.Close()
	return mergeOutputFile, err
}
