package srcreader

import (
	"bufio"
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

func (d *SrcDatabase) openTableRelatedFile(filename string) (*bufio.Reader, error) {
	file, err := os.Open(d.srcdbpath + "/" + filename)
	if err != nil {
		return nil, err
	}
	return bufio.NewReader(file), nil
}

func (d *SrcDatabase) OpenSQL(tablename string) (*bufio.Reader, error) {
	return d.openTableRelatedFile(tablename + ".sql")
}

func (d *SrcDatabase) OpenCSV(tablename string) (*bufio.Reader, error) {
	return d.openTableRelatedFile(tablename + ".csv")
}
