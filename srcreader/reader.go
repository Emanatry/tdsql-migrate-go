package srcreader

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
)

type Source struct {
	srcpath   string
	SrcName   string
	Databases []SrcDatabase
}

type SrcDatabase struct {
	srcdbpath      string
	tablePresorted []bool

	SrcName string
	Name    string
	Tables  []string
}

const PRESORT_PATH = "./presort/data/"
const SORTER_PROGRAM = "./presort/sortdata"
const MERGER_PROGRAM = "./presort/merge"

func doFileExists(path string) bool {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return false
	} else if err != nil {
		panic(err)
	}
	return true
}

func (db *SrcDatabase) getPresortMarkFile(table string) string {
	return PRESORT_PATH + db.SrcName + "/" + db.Name + "/" + table + ".presorted"
}

func (db *SrcDatabase) getPresortOutputFile(table string) string {
	return PRESORT_PATH + db.SrcName + "/" + db.Name + "/" + table + ".csv"
}

func Open(srcpath string, srcname string) (*Source, error) {
	if srcpath[len(srcpath)-1:] != "/" {
		srcpath = srcpath + "/"
	}

	src := &Source{
		srcpath: srcpath,
		SrcName: srcname,
	}

	files, err := ioutil.ReadDir(srcpath)

	if err != nil {
		return nil, err
	}

	// generate a list of source database names & table names based on source data filenames.
	for _, file := range files {

		var srcdb = SrcDatabase{
			Name:    file.Name(),
			SrcName: src.SrcName,

			srcdbpath: srcpath + file.Name() + "/",
		}

		tablefiles, err := ioutil.ReadDir(srcdb.srcdbpath)

		if err != nil {
			return nil, err
		}

		for _, tableFile := range tablefiles {
			tableFileName := tableFile.Name()
			if tableFileName[len(tableFileName)-4:] == ".sql" {
				table := tableFileName[:len(tableFileName)-4]
				srcdb.Tables = append(srcdb.Tables, table)
				srcdb.tablePresorted = append(srcdb.tablePresorted, doFileExists(srcdb.getPresortMarkFile(table)))
			}
		}

		src.Databases = append(src.Databases, srcdb)
	}

	return src, nil
}

func (d *SrcDatabase) ReadSQL(tablename string) (sqlContent []byte, err error) {
	sqlContent, err = ioutil.ReadFile(d.srcdbpath + "/" + tablename + ".sql")
	if err != nil {
		return
	}

	return
}

func (d *SrcDatabase) IsTablePresorted(table string) bool {
	for i, tb := range d.Tables {
		if table == tb {
			return d.tablePresorted[i]
		}
	}
	return false
}

func (d *SrcDatabase) OpenCSV(tablename string, seek int64) (*bufio.Reader, error) {
	path := d.srcdbpath + "/" + tablename + ".csv"
	if d.IsTablePresorted(tablename) {
		path = d.getPresortOutputFile(tablename)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if seek != 0 {
		file.Seek(seek, 0)
	}

	return bufio.NewReader(file), nil
}

func (d *SrcDatabase) determinePKColumnCount(table string) (int, error) {
	sql, err := d.ReadSQL(table)
	if err != nil {
		return -1, err
	}
	sqlstr := string(sql)
	// dirty but works
	if strings.Contains(sqlstr, "PRIMARY KEY (`id`)") {
		return 1, nil
	}
	if strings.Contains(sqlstr, "PRIMARY KEY (`id`,`a`)") {
		return 2, nil
	}
	return 3, nil
}

func pkColumnCountToArg(cnt int) string {
	switch cnt {
	case 1:
		return "id"
	case 2:
		return "id_a"
	default:
		return "id_a_b"
	}
}

func (s *Source) PresortDatabase() error {
	err := os.MkdirAll(PRESORT_PATH+s.SrcName, 0755)
	if err != nil {
		return err
	}
	for _, db := range s.Databases {
		err := os.MkdirAll(PRESORT_PATH+s.SrcName+"/"+db.Name, 0755)
		if err != nil {
			return err
		}
		for i, table := range db.Tables {
			if db.tablePresorted[i] {
				continue
			}
			colcnt, err := db.determinePKColumnCount(table)
			if err != nil {
				return err
			}
			fmt.Printf("@ presorting %s %s.%s (%s)\n", s.SrcName, db.Name, table, pkColumnCountToArg(colcnt))
			// presort files by running c++ sorting program
			cmd := exec.Command(SORTER_PROGRAM, db.srcdbpath+"/"+table+".csv", db.getPresortOutputFile(table), pkColumnCountToArg(colcnt))
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

			db.tablePresorted[i] = true
		}
	}
	return nil
}

func MergeSortedSource(a *Source, b *Source) error {
	root := PRESORT_PATH + "merged"
	err := os.MkdirAll(root, 0755)
	if err != nil {
		return err
	}
	for i, dba := range a.Databases {
		dbb := &b.Databases[i]
		dbroot := root + "/" + dba.Name
		err := os.MkdirAll(dbroot, 0755)
		if err != nil {
			return err
		}
		for _, table := range dba.Tables {
			markfile := dbroot + "/" + table + ".mark"
			if doFileExists(markfile) {
				continue
			}
			colcnt, err := dba.determinePKColumnCount(table)
			if err != nil {
				return err
			}
			fmt.Printf("@ merging %s.%s (%s)\n", dba.Name, table, pkColumnCountToArg(colcnt))
			sql, err := dba.ReadSQL(table)
			if err != nil {
				return err
			}
			err = ioutil.WriteFile(dbroot+"/"+table+".sql", sql, 0755)
			if err != nil {
				return err
			}
			cmd := exec.Command(MERGER_PROGRAM, dba.getPresortOutputFile(table), dbb.getPresortOutputFile(table), dbroot+"/"+table+".csv", pkColumnCountToArg(colcnt))
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				return err
			}
			err = cmd.Start()
			if err != nil {
				return err
			}
			out, err := ioutil.ReadAll(stdout)
			if err != nil {
				return err
			}
			println(string(out))
			cmd.Wait()

			// create mark file for merged tables
			f, err := os.Create(markfile)
			if err != nil {
				return err
			}
			f.Close()
		}
	}
	return nil
}
