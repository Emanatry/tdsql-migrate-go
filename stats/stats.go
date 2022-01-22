package stats

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var DevSuppressLog bool

var bytesMigratedSum int
var numCommitSum int
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

func ReportCommit() {
	statlock.Lock()
	numCommitSum++
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

// returns memFree, memAvail, swapFree, swapTotal in kB
func getMemStats() (total int, free int, available int) {
	meminfo, err := ioutil.ReadFile("/proc/meminfo")
	total = -1
	free = -1
	available = -1
	if err != nil {
		fmt.Println("failed reading /proc/meminfo: " + err.Error())
		return
	}

	fmt.Sscanf(string(meminfo), "MemTotal: %d kB\nMemFree: %d kB\nMemAvailable: %d kB", &total, &free, &available)
	return
}

func StartStatsReportingGoroutine(db *sql.DB) *bool {
	var doExit bool = false
	go func() {
		var lastIdle, lastTotal uint64
		for !doExit {
			idle, total := GetCPUSample() // only works on linux
			stat := db.Stats()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			_, free, available := getMemStats()
			fmt.Printf("@stats: %v, idle: %d, inUse: %d, open: %d, waitDuration(s): %d, aggSpeed(KB/s): %.2f, cpu(%%): %.2f, heap(MB): %d, memFree(MB): %d, memAvail(MB): %d, nCommit: %d\n",
				time.Now().Format(time.RFC3339), stat.Idle, stat.InUse, stat.OpenConnections, int(stat.WaitDuration.Seconds()), CalculateAggregateSpeedSinceLast(),
				(1-float64(idle-lastIdle)/float64(total-lastTotal))*100,
				m.HeapAlloc/1024/1024,
				free/1204,
				available/1024,
				numCommitSum,
			)

			numCommitSum = 0
			lastIdle = idle
			lastTotal = total
			time.Sleep(5 * time.Second)
		}
	}()
	return &doExit
}
