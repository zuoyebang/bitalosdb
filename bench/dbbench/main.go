package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var executeWriteNums = int32(0)
var executeWriteTotalTime = uint64(0)
var writeFailNums = int32(0)

var executeReadNums = int32(0)
var executeReadTotalTime = uint64(0)
var executeGetTime uint64

var executeDeleteNums = int32(0)
var deleteFailNums = int32(0)
var executeDeleteTotalTime = uint64(0)

var command *string
var goNum *int
var cpuNum *int
var globalKeyIndexMax *int
var loop *int
var dbType *int
var showMemory *int
var valueType *int
var dbName *string

var delayExit *int
var writeNum *int
var printPeriod int32
var vlen *int

var notFoundNum int32
var deleteButFoundNum int32

var commandFinish = 0

func main() {
	defer func() {
		switch *dbType {
		case DB_BITALOS:
			time.Sleep(3 * time.Second)
			bitalosDB.db.Close()
		}
	}()

	printPeriod = 1000000

	goNum = flag.Int("gonum", 48, "input goroutine nums")
	cpuNum = flag.Int("cpus", 48, "cpu num")
	command = flag.String("command", "command", "-command=r|randomWrite")
	globalKeyIndexMax = flag.Int("max", 1000000, "input key max")
	dbType = flag.Int("db", 0, "database: 1=bitalos")
	dbName = flag.String("dbname", "", "db name")
	loop := flag.Int("loop", 0, "infinite loop: 0|1 (only support r/wr command)")

	pprofEnable := flag.Int("pprof", 0, "pprof: 0|1")
	showMemory = flag.Int("showMemory", 1, "show memory: 1=show, 0=no show")
	valueType = flag.Int("valueType", 1, "value type: 1=rand")
	delayExit = flag.Int("delayExit", 0, "delay exit")
	writeNum = flag.Int("writeNum", 0, "writeNum")
	vlen = flag.Int("vlen", 10, "valuesize")

	flag.Parse()

	if *dbName == "" {
		panic("db name is empty")
	}

	runtime.GOMAXPROCS(*cpuNum)
	if err := writeCgroupCpuConfig("/sys/fs/cgroup/cpu/stored/paper_bench", *cpuNum); err != nil {
		panic(any(err))
	}

	initDB()

	goNumInt := *goNum
	go func() {
		if *pprofEnable == 1 {
			if err := http.ListenAndServe(pprofPort, nil); err != nil {
				fmt.Println("pprof", err)
			}
		}
	}()

	go func() {
		if *showMemory == 0 {
			return
		}

		for {
			usage, err := GetUsage()
			if err == nil {
				fmt.Println("mem total(GB):", float64(usage.MemTotal())/1073741824, "shr(GB):", float64(usage.MemShr())/1073741824)
			}
			time.Sleep(60 * time.Second)
		}
	}()

	if *command == "r" {
		switch *dbType {
		case DB_BITALOS:
			executeReadBitalosDB(goNumInt)
			resetResource()
			if *loop == -1 {
				for {
					executeReadBitalosDB(goNumInt)
					resetResource()
				}
			} else if *loop > 0 {
				loopNum := *loop
				for loopNum > 0 {
					executeReadBitalosDB(goNumInt)
					resetResource()
					loopNum--
				}
			}
			commandFinish = COMMAND_FINISH
		default:
			fmt.Println("only support db: bitalos")
		}
	} else if *command == "randomWrite" {
		switch *dbType {
		case DB_BITALOS:
			executeRandomWriteBitalosDB()
			commandFinish = COMMAND_FINISH
			resetResource()
		default:
			fmt.Println("only support db: bitalos")
		}
	}

	if *delayExit > 0 {
		time.Sleep(time.Duration(*delayExit) * time.Second)
	}
}

func executeRandomWriteBitalosDB() {
	threadNum := *goNum
	writeNum := *globalKeyIndexMax / threadNum

	time.Sleep(5 * time.Second)

	fmt.Println("bitalos write start")

	now := time.Now().UnixNano()
	randObjs := make([]*rand.Rand, threadNum)
	stats := make([]threadStat, threadNum)
	for i := 0; i < threadNum; i++ {
		stats[i] = threadStat{}
		randObjs[i] = rand.New(rand.NewSource(now + int64(i)))
	}

	wg := &sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < threadNum; i++ {
		wg.Add(1)
		go randomWriteBitalosDBByInt(wg, writeNum, randObjs[i], &stats[i])
	}
	wg.Wait()
	subTime := time.Now().Sub(start)

	executeNum := 0
	for i := 0; i < threadNum; i++ {
		executeNum += stats[i].executeNum
	}

	fmt.Printf("cost-time:%v ; qps: %v avgTime: %.2f(ns)\n", subTime, float64(executeNum)/subTime.Seconds(), float64(subTime.Nanoseconds())/float64(executeNum))
	fmt.Println("bitalos write end")
}

func randomWriteBitalosDBByInt(wg *sync.WaitGroup, writeNum int, randObj *rand.Rand, stat *threadStat) {
	defer func() {
		wg.Done()
	}()

	keyNum := writeNum
	step := keyNum / 10
	var key [18]byte
	var newKey []byte
	for i := 0; i < keyNum; i++ {
		id := randObj.Int() % (*globalKeyIndexMax)
		FillFullKey(key[:], len(key), id)
		newKey = key[:]
		newValue := makeValue(id)

		if i == 0 {
			fmt.Println("keyLen", len(newKey), "valueLen", len(newValue))
		}

		err := bitalosDB.db.Set(newKey, newValue, bitalosDB.wo)
		if err != nil {
			stat.errorNum++
		}

		if i > 0 && i%step == 0 {
			fmt.Println("execute write step:", i)
		}
	}
	stat.executeNum = keyNum
}

func executeReadBitalosDB(num int) {
	fmt.Println("bitalos read start")

	threadNum := *goNum
	eachReadNum := *globalKeyIndexMax / threadNum

	time.Sleep(5 * time.Second)

	randObjs := make([]*rand.Rand, threadNum)
	stats := make([]threadStat, threadNum)
	now := time.Now().UnixNano()
	for i := 0; i < threadNum; i++ {
		stats[i] = threadStat{}
		randObjs[i] = rand.New(rand.NewSource(now + int64(i)))
	}

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < num; i++ {
		wg.Add(1)
		go readBitalosDBByInt(&wg, nil, nil, eachReadNum, randObjs[i], &stats[i])
	}
	wg.Wait()
	subTime := time.Now().Sub(start)

	executeNum := 0
	for i := 0; i < threadNum; i++ {
		executeNum += stats[i].executeNum
	}

	fmt.Printf("cost-time:%v ; qps: %v avgTime: %.2f(ns) (%d / %d not found) threadNum:%d getCost:%d(ns) \n",
		subTime,
		float64(executeNum)/subTime.Seconds(),
		float64(subTime.Nanoseconds())/float64(executeNum),
		stats[0].errorNum,
		stats[0].executeNum,
		threadNum,
		executeGetTime,
	)
	fmt.Println("bitalos read end")
}

func readBitalosDBByInt(wg *sync.WaitGroup, keys [][]byte, idxList []int, readNum int, randObj *rand.Rand, stat *threadStat) {
	defer func() {
		wg.Done()
	}()
	if *vlen < 10 {
		panic("value size is >= 10")
	}

	key := make([]byte, 18)
	for i := 0; i < readNum; i++ {
		FillFullKey(key, len(key), randObj.Int()%(*globalKeyIndexMax))

		s := time.Now()
		val, closer, _ := bitalosDB.db.Get(key)
		atomic.AddUint64(&executeGetTime, uint64(time.Since(s).Nanoseconds()))
		if len(val) == 0 {
			stat.errorNum++
		}
		if closer != nil {
			closer()
		}
	}
	stat.executeNum = readNum
}

func printStats() {
	if executeWriteNums > 0 {
		fmt.Printf("write: runNums: %d write fails: %d, avgTime: %.2f(ns)\n", executeWriteNums, writeFailNums, float64(executeWriteTotalTime)/float64(executeWriteNums))
	}

	if executeReadNums > 0 {
		fmt.Printf("read: runNums: %d avgTime: %.2f(ns)\n", executeReadNums, float64(executeReadTotalTime)/float64(executeReadNums))
		fmt.Printf("not found: %d deleteButFound: %d\n", notFoundNum, deleteButFoundNum)
	}

	if executeWriteNums > 0 && executeReadNums > 0 {
		fmt.Printf("wr: runNums: %d avgTime: %.2f(ns)\n", executeReadNums+executeWriteNums, float64(executeWriteTotalTime+executeReadTotalTime)/float64(executeWriteNums+executeReadNums))
	}

	if executeDeleteNums > 0 {
		fmt.Printf("delete nums: %d, delete fails: %d avgTime: %.2f(ns)\n", executeDeleteNums, deleteFailNums, float64(executeDeleteTotalTime)/float64(executeDeleteNums))
	}
	fmt.Println("")
}

func resetWriteResource() {
	executeWriteNums = 0
	executeWriteTotalTime = 0
	writeFailNums = 0
}

func resetReadResource() {
	executeReadNums = 0
	executeReadTotalTime = 0
	notFoundNum = 0
	deleteButFoundNum = 0
}

func resetDeleteResouce() {
	executeDeleteNums = 0
	deleteFailNums = 0
}

func resetResource() {
	resetWriteResource()
	resetReadResource()
	resetDeleteResouce()

	atomickeyIndex = 0
}
