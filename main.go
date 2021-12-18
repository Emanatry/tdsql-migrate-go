package main

import (
	"database/sql"
	"flag"
	"fmt"
	"time"

	"github.com/Emanatry/tdsql-migrate-go/migrator"
	"github.com/Emanatry/tdsql-migrate-go/srcreader"
	_ "github.com/go-sql-driver/mysql"
)

var dataPath *string
var dstIP *string
var dstPort *int
var dstUser *string
var dstPassword *string

func main() {

	// parse arguments
	println("\n======== parse arguments ========")

	dataPath = flag.String("data_path", "/tmp/data/", "dir path of source data")
	dstIP = flag.String("dst_ip", "", "ip of dst database address")
	dstPort = flag.Int("dst_port", 0, "port of dst database address")
	dstUser = flag.String("dst_user", "", "user name of dst database")
	dstPassword = flag.String("dst_password", "", "password of dst database")

	flag.Parse()

	fmt.Printf("data path:%v\n", *dataPath)
	fmt.Printf("dst ip:%v\n", *dstIP)
	fmt.Printf("dst port:%v\n", *dstPort)
	fmt.Printf("dst user:%v\n", *dstUser)
	fmt.Printf("dst password:%v\n", *dstPassword)

	// open sources
	println("\n======== open sources ========")

	// TODO: 一定是只有两个 source 吗？

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

	DSN := fmt.Sprintf("%s:%s@(%s:%d)/?parseTime=true&loc=Local", *dstUser, *dstPassword, *dstIP, *dstPort)
	println("DSN: " + DSN)

	db, err := sql.Open("mysql", DSN)
	if err != nil {
		panic(err)
	}

	db.SetConnMaxLifetime(time.Minute * 5)
	db.SetMaxOpenConns(40)
	db.SetMaxIdleConns(30)
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

	println("\n======== migrate database ========")

	// 准备迁移目标实例的环境，创建迁移过程中需要的临时表等。
	migrator.PrepareTargetDB(db)

	println("")

	if err := migrator.MigrateSource(srca, db); err != nil {
		panic(err)
	}
	if err := migrator.MigrateSource(srcb, db); err != nil {
		panic(err)
	}

	if err := migrator.PostJob(db); err != nil {
		// panic(err)
		println("error: " + err.Error()) // nah, just continue anyway.
	}

	db.Close() // note: do not close the database after adding worker goroutines.
}
