package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/Emanatry/tdsql-migrate-go/migrator"
	"github.com/Emanatry/tdsql-migrate-go/srcreader"
	"github.com/Emanatry/tdsql-migrate-go/stats"
	_ "github.com/go-sql-driver/mysql"
)

var dataPath *string
var dstIP *string
var dstPort *int
var dstUser *string
var dstPassword *string
var suppressLog *bool

func main() {
	// for distinguishing between different builds and logs
	label, err := ioutil.ReadFile("./label.txt")
	if err == nil {
		fmt.Printf("======LABEL OF THIS BUILD======\n%s===============================\n", string(label))
	}

	// parse arguments
	println("\n======== parse arguments ========")

	dataPath = flag.String("data_path", "/tmp/data/", "dir path of source data")
	dstIP = flag.String("dst_ip", "", "ip of dst database address")
	dstPort = flag.Int("dst_port", 0, "port of dst database address")
	dstUser = flag.String("dst_user", "", "user name of dst database")
	dstPassword = flag.String("dst_password", "", "password of dst database")
	suppressLog = flag.Bool("suppress_log", false, "do suppress dev logs")

	flag.Parse()

	fmt.Printf("data path:%v\n", *dataPath)
	fmt.Printf("dst ip:%v\n", *dstIP)
	fmt.Printf("dst port:%v\n", *dstPort)
	fmt.Printf("dst user:%v\n", *dstUser)
	fmt.Printf("dst password:%v\n", *dstPassword)
	stats.DevSuppressLog = *suppressLog

	if (*dataPath)[len(*dataPath)-1:] != "/" {
		*dataPath += "/"
	}

	var srcdirs []string
	dir, err := ioutil.ReadDir(*dataPath)
	if err != nil {
		// do nothing
	} else {
		for _, v := range dir {
			srcdirs = append(srcdirs, v.Name())
		}
	}

	fmt.Printf("directories in data_path: %v", srcdirs)

	// open sources
	println("\n======== open sources ========")

	srca, err := srcreader.Open(*dataPath+"src_a", "src_a")
	if err != nil {
		println("failed opening source a: " + err.Error())
		return
	}

	srcb, err := srcreader.Open(*dataPath+"src_b", "src_b")
	if err != nil {
		println("failed opening source b: " + err.Error())
		return
	}

	fmt.Printf("source a databases: %v\n", srca.Databases)
	fmt.Printf("source b databases: %v\n", srcb.Databases)

	// open database connection
	println("\n======== open database connection ========")

	DSN := fmt.Sprintf("%s:%s@(%s:%d)/?parseTime=true&loc=Local&autocommit=false", *dstUser, *dstPassword, *dstIP, *dstPort)
	println("DSN: " + DSN)

	db, err := sql.Open("mysql", DSN)
	if err != nil {
		panic(err)
	}

	db.SetConnMaxIdleTime(-1)
	db.SetConnMaxLifetime(-1)
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100)
	db.Ping()

	println("connection to database succesfully established!")

	// test database connection
	println("\n======== test database connection ========")

	rows, err := db.Query("SHOW DATABASES;")
	if err != nil {
		panic(err)
	}

	fmt.Printf("remote databases: \n")

	for rows.Next() {
		var dbname string
		rows.Scan(&dbname)
		println(" - " + dbname)
	}

	rows.Close()

	fmt.Printf("database stats: \n%+v\n", db.Stats())

	var doExit *bool = stats.StartStatsReportingGoroutine(db)
	println("\n======== this implementation sorts and merges on the fly ========")

	println("\n======== migrate database ========")

	doCreateTable := true
	// workaround for a judge env bug where not all tables from a previous migration attempt is dropped
	if _, err := os.Stat("./migration_inprogress.txt"); errors.Is(err, os.ErrNotExist) {
		f, err := os.Create("./migration_inprogress.txt")
		if err != nil {
			panic(fmt.Sprintf("failed creating migration_inprogress.txt: %s\n", err))
		}
		f.Write([]byte(time.Now().String()))
		if err = migrator.PostJobDropMetaMigration(db); err != nil {
			fmt.Printf("failed dropping meta_migration: %s\n", err.Error())
		}
		f.Close()
	} else {
		fmt.Printf("migration_inprogress.txt exists.\n")
		doCreateTable = false
	}

	// ?????????????????????????????????????????????????????????????????????????????????
	migrator.PrepareTargetDB(db)

	println("\n======== starting backgound presort & merge ========")
	srcreader.StartBackgoundPresortMerge(srca, srcb)

	if err := migrator.MigrateSource(srca, srcb, db, DSN, doCreateTable); err != nil {
		panic(err)
	}

	// ==== for migrating without local dedup:
	// if err := migrator.MigrateSource(srca, db, false); err != nil {
	// 	panic(err)
	// }
	// if err := migrator.MigrateSource(srcb, db, false); err != nil {
	// 	panic(err)
	// }

	// ==== for migrating a single table:
	// if err := migrator.MigrateTable(&srcb.Databases[0], "4", db, false); err != nil {
	// 	panic(err)
	// }

	// ==== Note: disabled postjob to save some time in the competition.
	// if err := migrator.PostJob(db); err != nil {
	// 	// panic(err)
	// 	println("error: " + err.Error()) // nah, just continue anyway.
	// }

	if err := migrator.PostJobDropMetaMigration(db); err != nil {
		fmt.Printf("failed dropping meta migration: %s\n", err.Error())
	}

	db.Close()
	*doExit = true

	println("all done, exiting......")
	os.Remove("./migration_inprogress.txt")
	os.Exit(0)
}
