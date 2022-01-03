package migrator

import (
	"database/sql"
	"errors"
	"fmt"
)

// run after all databases and tables from all sources are fully migrated
func PostJob(db *sql.DB) error {
	println("* postjob started")

	tx0, err := db.Begin()
	if err != nil {
		return errors.New("postjob failed creating transaction tx0: " + err.Error())
	}

	res, err := tx0.Query("SELECT dbname, tablename FROM meta_migration.migration_log WHERE `temp_prikey` = 1 GROUP BY dbname, tablename;")
	if err != nil {
		return errors.New("postjob failed selecting temp_prikey to remove from migration log: " + err.Error())
	}

	var dbnames, tablenames []string
	for res.Next() {
		var dbname, tablename string
		res.Scan(&dbname, &tablename)
		dbnames = append(dbnames, dbname)
		tablenames = append(tablenames, tablename)
	}

	res.Close()

	for i, dbname := range dbnames {
		tablename := tablenames[i]
		fmt.Printf("* removing temp_prikey of %s.%s from migration_log\n", dbname, tablename)
		_, err = db.Exec("UPDATE meta_migration.migration_log SET temp_prikey = 0 WHERE dbname = ? AND tablename = ?;", dbname, tablename)
		if err != nil {
			return fmt.Errorf("postjob failed updating migration log for %s.%s after removing temp_prikey: %s", dbname, tablename, err.Error())
		}
	}

	err = tx0.Commit()
	if err != nil {
		return err
	}

	// DDL statement triggers a implicit commit, so should do it after updating meta_migration
	// HOWEVER, bad thing could happen if the program is stopped right now, and might result in extra primary keys not being deleted.

	fmt.Printf("=== all temp_prikeys has been committed into migration_log\n")
	fmt.Printf("=== start actually dropping primary keys.\n") // if the program stops right now, primary keys might not have been dropped yet.
	// TODO: potential solution: use a log locally to record if each temp primary key has actually been dropped yet.

	for i, dbname := range dbnames {
		tablename := tablenames[i]
		fmt.Printf("* removing temp prikey from %s.%s\n", dbname, tablename)
		_, err = db.Exec(fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PRIMARY KEY;", dbname, tablename))
		if err != nil {
			// return fmt.Errorf("postjob failed dropping temp primary key for %s.%s: %s", dbname, tablename, err.Error())
			fmt.Printf("error: postjob failed dropping temp primary key for %s.%s: %s\n", dbname, tablename, err.Error())
			// this error has been temporarily softened so that it would not cause a panic at the very last stage of the operation
		}
	}

	println("* postjob finished")
	return nil
}

func PostJobDropMetaMigration(db *sql.DB) error {
	println("* postjob started drop meta_migration")

	_, err := db.Exec("DROP DATABASE meta_migration;")
	if err != nil {
		return err
	}

	println("* postjob drop meta_migration finished")
	return nil
}
