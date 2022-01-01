package stats

import (
	"bufio"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os/exec"
	"runtime"
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

// returns memFree, memAvail, swapFree, swapTotal in MB
func getMemStats() (int, int, int, int) {
	cmd := exec.Command("free", "--mega")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Println("failed redirecting output of free: " + err.Error())
		return -1, -1, -1, -1
	}

	cmd.Start()

	r := bufio.NewReader(stdout)
	r.ReadLine() // skip first line
	lineBA, _, err := r.ReadLine()
	if err != nil {
		fmt.Println("failed reading output of free: " + err.Error())
		return -1, -1, -1, -1
	}
	var free, available, discarded, swap, totalswap int
	fmt.Sscanf(string(lineBA), "Mem: %d %d %d %d %d %d", &discarded, &discarded, &free, &discarded, &discarded, &available)
	lineBA, _, err = r.ReadLine()
	if err != nil {
		fmt.Println("failed reading output of free: " + err.Error())
		return -1, -1, -1, -1
	}
	fmt.Sscanf(string(lineBA), "Swap: %d %d %d", &totalswap, &discarded, &swap)

	stdout.Close()
	cmd.Wait()
	return free, available, swap, totalswap
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

			free, available, swap, _ := getMemStats()
			fmt.Printf("@stats: %v, idle: %d, inUse: %d, open: %d, waitDuration(s): %d, aggSpeed(KB/s): %.2f, cpu(%%): %.2f, heap(MB): %d, memFree(MB): %d, memAvail(MB): %d, swapFree(MB): %d\n",
				time.Now().Format(time.RFC3339), stat.Idle, stat.InUse, stat.OpenConnections, int(stat.WaitDuration.Seconds()), CalculateAggregateSpeedSinceLast(),
				(1-float64(idle-lastIdle)/float64(total-lastTotal))*100,
				m.HeapAlloc/1024/1024,
				free,
				available,
				swap, // actual judge env appears to have no swap, so this is always 0
			)

			lastIdle = idle
			lastTotal = total
			time.Sleep(5 * time.Second)
		}
	}()
	return &doExit
}
