package srcreader

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"os"
)

type Source struct {
	srcpath   string
	SrcName   string
	Databases []SrcDatabase
}

type SrcDatabase struct {
	srcdbpath string
	SrcName   string
	Name      string
	Tables    []string
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
			Name:      file.Name(),
			SrcName:   src.SrcName,
			srcdbpath: srcpath + file.Name() + "/",
		}

		tablefiles, err := ioutil.ReadDir(srcdb.srcdbpath)

		if err != nil {
			return nil, err
		}

		for _, tableFile := range tablefiles {
			tableFileName := tableFile.Name()
			if tableFileName[len(tableFileName)-4:] == ".sql" {
				srcdb.Tables = append(srcdb.Tables, tableFileName[:len(tableFileName)-4])
			}
		}

		src.Databases = append(src.Databases, srcdb)
	}

	return src, nil
}

func (d *SrcDatabase) ReadSQL(tablename string) (sqlContent []byte, isOrdinaryKey bool, err error) {
	// TODO: this is a dirty way to check for a ordinary key! this is NOT robust.
	// this only detects (unreliably) if any ordinary key exists on the table,
	// and does not account for the case where two indecies exists on the same table,
	// with one being PRIMARY/UNIQUE and the other being ordinary key.
	var ordikeystr = []byte("\n  KEY (`")

	sqlContent, err = ioutil.ReadFile(d.srcdbpath + "/" + tablename + ".sql")
	if err != nil {
		return
	}

	isOrdinaryKey = bytes.Contains(sqlContent, ordikeystr)
	return
}

func (d *SrcDatabase) OpenCSV(tablename string, seek int64) (*bufio.Reader, error) {
	file, err := os.Open(d.srcdbpath + "/" + tablename + ".csv")
	if err != nil {
		return nil, err
	}
	if seek != 0 {
		file.Seek(seek, 0)
	}

	return bufio.NewReader(file), nil
}
