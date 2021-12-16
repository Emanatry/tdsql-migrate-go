package srcreader

import (
	"io/ioutil"
)

type Source struct {
	Databases []SrcDatabase
}

type SrcDatabase struct {
	Name   string
	Tables []string
}

func Open(srcpath string) (*Source, error) {
	src := &Source{}

	files, err := ioutil.ReadDir(srcpath)

	if err != nil {
		return nil, err
	}

	// generate a list of source database names & table names based on source data filenames.
	for _, file := range files {
		var db SrcDatabase
		db.Name = file.Name()

		tablefiles, err := ioutil.ReadDir(srcpath + "/" + file.Name())

		if err != nil {
			return nil, err
		}

		for _, tableFile := range tablefiles {
			tableFileName := tableFile.Name()
			if tableFileName[len(tableFileName)-4:] == ".sql" {
				db.Tables = append(db.Tables, tableFileName[:len(tableFileName)-4])
			}
		}

		src.Databases = append(src.Databases, db)
	}

	return src, nil
}

func (d *SrcDatabase) OpenCSV() error {
	panic("unimplemented: OpenCSV()")
}
