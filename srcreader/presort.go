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

func (s *Source) PresortDatabase() error {
	err := os.MkdirAll(PRESORT_PATH+s.SrcName, 0755)
	if err != nil {
		return err
	}
	rateLimitSem := semaphore.New(CONCURRENT_PRESORT_JOB)
	var wg sync.WaitGroup

	for _, db := range s.Databases {
		err := os.MkdirAll(PRESORT_PATH+s.SrcName+"/"+db.Name, 0755)
		if err != nil {
			return err
		}
		for i, table := range db.Tables {
			wg.Add(1)
			go func(db *SrcDatabase, table string, i int) {
				defer wg.Done()
				if db.tablePresorted[i] {
					return
				}

				rateLimitSem.Acquire() // rate limit
				defer rateLimitSem.Release()

				coltype, err := db.determinePKColumnType(table)
				if err != nil {
					panic(err)
				}
				fmt.Printf("@ presorting %s %s.%s (%s)\n", s.SrcName, db.Name, table, coltype)
				// presort files by running c++ sorting program
				cmd := exec.Command(SORTER_PROGRAM, db.srcdbpath+"/"+table+".csv", db.getPresortOutputFile(table), coltype)
				err = cmd.Run()
				if err != nil {
					panic(err)
				}

				// create mark file for presorted tables
				f, err := os.Create(db.getPresortMarkFile(table))
				if err != nil {
					panic(err)
				}
				f.Close()

				db.tablePresorted[i] = true
			}(db, table, i)
		}
	}
	wg.Wait()
	return nil
}

func MergeSortedSource(a *Source, b *Source) error {
	root := PRESORT_PATH + "merged"
	err := os.MkdirAll(root, 0755)
	if err != nil {
		return err
	}
	rateLimitSem := semaphore.New(CONCURRENT_PRESORT_JOB)
	var wg sync.WaitGroup

	for i, dba := range a.Databases {
		dbb := b.Databases[i]
		dbroot := root + "/" + dba.Name
		err := os.MkdirAll(dbroot, 0755)
		if err != nil {
			return err
		}
		for _, table := range dba.Tables {
			wg.Add(1)
			go func(dba *SrcDatabase, dbb *SrcDatabase, table string) {
				defer wg.Done()
				markfile := dbroot + "/" + table + ".mark"
				if doFileExists(markfile) {
					return
				}

				rateLimitSem.Acquire()
				defer rateLimitSem.Release()

				coltype, err := dba.determinePKColumnType(table)
				if err != nil {
					panic(err)
				}
				fmt.Printf("@ merging %s.%s (%s)\n", dba.Name, table, coltype)
				sql, err := dba.ReadSQL(table)
				if err != nil {
					panic(err)
				}
				err = ioutil.WriteFile(dbroot+"/"+table+".sql", sql, 0755)
				if err != nil {
					panic(err)
				}
				cmd := exec.Command(MERGER_PROGRAM, dba.getPresortOutputFile(table), dbb.getPresortOutputFile(table), dbroot+"/"+table+".csv", coltype)
				stdout, err := cmd.StdoutPipe()
				if err != nil {
					panic(err)
				}
				err = cmd.Start()
				if err != nil {
					panic(err)
				}
				out, err := ioutil.ReadAll(stdout)
				if err != nil {
					panic(err)
				}
				println(string(out))
				cmd.Wait()

				// create mark file for merged tables
				f, err := os.Create(markfile)
				if err != nil {
					panic(err)
				}
				f.Close()
			}(dba, dbb, table)
		}
	}
	wg.Wait()
	return nil
}
