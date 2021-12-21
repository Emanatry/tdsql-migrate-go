package main

import (
	"crypto/md5"
	"fmt"
	"hash/fnv"
	"io"
	"time"

	"github.com/Emanatry/tdsql-migrate-go/srcreader"
)

func main() {
	fmt.Printf("preflight running...\n")
	testHashSpeedFnv()
	testHashSpeedMd5()
}

const BATCHSIZE = 3000

func testHashSpeedFnv() {
	f := fnv.New64a()
	f.Reset()

	srca, err := srcreader.Open("../data/src_a", "src_a")
	if err != nil {
		println("failed opening source a: " + err.Error())
		return
	}

	srcdb := srca.Databases[0]
	csv, err := srcdb.OpenCSV("1", 0)
	if err != nil {
		panic(err)
	}

	for t := 0; t < 10; t++ {
		t1 := time.Now()
		for i := 0; i < BATCHSIZE; i++ {
			line, err := csv.ReadBytes('\n')
			if err == io.EOF {
				return
			}
			f.Reset()
			f.Write(line)
			f.Sum64()

			if err != nil {
				panic(err.Error())
			}
		}
		fmt.Printf("%d fnv hashes took %dus\n", BATCHSIZE, time.Since(t1).Microseconds())
	}

}

func testHashSpeedMd5() {
	f := md5.New()
	f.Reset()

	srca, err := srcreader.Open("../data/src_a", "src_a")
	if err != nil {
		println("failed opening source a: " + err.Error())
		return
	}

	srcdb := srca.Databases[0]
	csv, err := srcdb.OpenCSV("1", 0)
	if err != nil {
		panic(err)
	}

	for t := 0; t < 10; t++ {
		t1 := time.Now()
		for i := 0; i < BATCHSIZE; i++ {
			line, err := csv.ReadBytes('\n')
			if err == io.EOF {
				return
			}
			f.Reset()
			f.Sum(line)

			if err != nil {
				panic(err.Error())
			}
		}
		fmt.Printf("%d md5 hashes took %dus\n", BATCHSIZE, time.Since(t1).Microseconds())
	}

}
