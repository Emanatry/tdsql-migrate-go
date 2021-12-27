package stats

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"
)

var bytesMigratedSum int
var lastTimeCalculated time.Time = time.Now()

var statlock sync.Mutex

func ReportBytesMigrated(bytesMigrated int) {
	if bytesMigrated <= 0 {
		return
	}
	statlock.Lock()
	bytesMigratedSum += bytesMigrated
	statlock.Unlock()
}

func CalculateAggregateSpeedSinceLast() float32 {
	now := time.Now()
	aggSpeed := float32(bytesMigratedSum) / float32(now.Sub(lastTimeCalculated).Seconds()) / 1024
	lastTimeCalculated = now
	bytesMigratedSum = 0
	return aggSpeed
}

// by @bertimus9 on stackoverflow.
// https://stackoverflow.com/questions/11356330/how-to-get-cpu-usage
// only works on linux
func GetCPUSample() (idle, total uint64) {
	contents, err := ioutil.ReadFile("/proc/stat")
	if err != nil {
		return
	}
	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if fields[0] == "cpu" {
			numFields := len(fields)
			for i := 1; i < numFields; i++ {
				val, err := strconv.ParseUint(fields[i], 10, 64)
				if err != nil {
					fmt.Println("Error: ", i, fields[i], err)
				}
				total += val // tally up all the numbers to get total ticks
				if i == 4 {  // idle is the 5th field in the cpu line
					idle = val
				}
			}
			return
		}
	}
	return
}
