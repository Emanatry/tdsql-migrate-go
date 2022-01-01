package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)

type dat struct {
	key  int
	data [][]byte
}

var dats []dat

func main() {
	r := bufio.NewReader(os.Stdin)
	of, err := os.Create("./1_sorted_go.csv")
	o := bufio.NewWriter(of)

	if err != nil {
		panic(err)
	}
	t1 := time.Now()
	for {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		line = line[:len(line)-1]
		buf := bytes.Split(line, []byte(","))
		key, err := strconv.Atoi(string(buf[0]))
		if err != nil {
			panic(err)
		}
		dats = append(dats, dat{
			key:  key,
			data: buf,
		})
	}
	t2 := time.Now()
	sort.Slice(dats, func(i, j int) bool {
		if dats[i].key == dats[j].key {
			return bytes.Compare(dats[i].data[2], dats[j].data[2]) == -1
		}
		return dats[i].key < dats[j].key
	})
	t3 := time.Now()
	for _, v := range dats {
		for i, col := range v.data {
			if i != 0 {
				o.WriteRune(',')
			}
			o.Write(col)
		}
		o.WriteRune('\n')
	}
	o.Flush()
	of.Close()
	t4 := time.Now()
	fmt.Printf("read: %dms\n", t2.Sub(t1).Milliseconds())
	fmt.Printf("sort: %dms\n", t3.Sub(t2).Milliseconds())
	fmt.Printf("write: %dms\n", t4.Sub(t3).Milliseconds())
}
