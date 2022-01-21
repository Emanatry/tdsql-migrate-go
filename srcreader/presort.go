package srcreader

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/Emanatry/tdsql-migrate-go/semaphore"
)

const PRESORT_PATH = "./presort/data/"
const SORTMERGER_PROGRAM = "./presort/sortmerge"

// limit the total amout of concurrent presort job to avoid OOM.
const CONCURRENT_PRESORT_JOB = 5

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

func (db *SrcDatabase) getTableDataFilePath(table string) string {
	return db.srcdbpath + "/" + table + ".csv"
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

var rateLimitSem = semaphore.New(CONCURRENT_PRESORT_JOB)
var sortMergeMutexMap = make(map[string]*sync.Mutex)
var sortMergeMutexMapLock sync.Mutex

func PresortAndMergeTable(dba *SrcDatabase, dbb *SrcDatabase, table string) (csvpath string, err error) {
	tableIndex := dba.getTableIndex(table)

	dbroot := PRESORT_PATH + "merged" + "/" + dba.Name
	mergeOutputFile := dbroot + "/" + table + ".csv"

	rateLimitSem.Acquire()
	defer rateLimitSem.Release()

	var mergeLock *sync.Mutex
	sortMergeMutexMapLock.Lock()
	if lock, ok := sortMergeMutexMap[mergeOutputFile]; !ok {
		mergeLock = &sync.Mutex{}
		sortMergeMutexMap[mergeOutputFile] = mergeLock
	} else {
		mergeLock = lock
	}
	sortMergeMutexMapLock.Unlock()

	// this is messy... presort and merging used to be two individual steps
	// we decided to combine them together (for reduced io time) at the last minute.
	if dba.tablePresorted[tableIndex] && dbb.tablePresorted[tableIndex] {
		return mergeOutputFile, nil
	}

	mergeLock.Lock()
	defer mergeLock.Unlock()

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
	fmt.Printf("@ presorting & merging %s.%s (%s)\n", dba.Name, table, coltype)
	sql, err := dba.ReadSQL(table)
	if err != nil {
		return "", err
	}
	err = ioutil.WriteFile(dbroot+"/"+table+".sql", sql, 0755)
	if err != nil {
		return "", err
	}
	cmd := exec.Command(SORTMERGER_PROGRAM, dba.getTableDataFilePath(table), dbb.getTableDataFilePath(table), mergeOutputFile, coltype)
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

	dba.tablePresorted[tableIndex] = true
	dbb.tablePresorted[tableIndex] = true

	return mergeOutputFile, err
}

func StartBackgoundPresortMerge(srca *Source, srcb *Source) {
	go func() {
		sortAndMergeTable := func(table string) {
			for i, dba := range srca.Databases {
				dbb := srcb.Databases[i]
				_, err := PresortAndMergeTable(dba, dbb, table)
				if err != nil {
					panic(err)
				}
			}
		}
		sortAndMergeTable("2")
		sortAndMergeTable("3")
		sortAndMergeTable("4")
	}()
}
