package migrator

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

const migrationLogRoot = "./migration_log"

func readSeekMigrationLog(src string, db string, table string) (seek int, err error) {
	logdir := strings.Join([]string{migrationLogRoot, src, db, table}, "/")
	err = os.MkdirAll(logdir, 0755)
	if err != nil {
		return -1, err
	}
	seekdata, err := ioutil.ReadFile(logdir + "/seek.txt")
	if os.IsNotExist(err) {
		return -2, nil
	}
	if err != nil {
		return -1, err
	}
	return strconv.Atoi(string(seekdata))
}

func writeSeekMigrationLog(src string, db string, table string, newseek int) error {
	logdir := strings.Join([]string{migrationLogRoot, src, db, table}, "/")
	err := os.MkdirAll(logdir, 0755)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(logdir+"/seek.txt", []byte(strconv.Itoa(newseek)), 0755) // could fail if killed?
	if err != nil {
		return err
	}
	return nil
}
