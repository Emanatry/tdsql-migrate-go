package stats

import (
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
