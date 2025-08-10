# Benchmark

This YCSB benchmark tool will test the random read and write performance of BitalosDB at different workloads.

## Build

```
./make
```

## Run benchmark

1, First insert testdata into the BitalosDB

```
./main bench ycsb testdb --workload A --num-ops=100 --initial-keys=10485760 --disable-wal=true --keys=zipf --duration=1200s
```

2, Test the performance at different workloads.

```
workload=A
./main bench ycsb testdb --workload ${workload} --prepopulated-keys=10485760 --num-ops=1000000 --disable-wal=true --keys=zipf --duration=1200s > log/work${workload}.log
# The {worload} var can be set from A to F
# ./main bench ycsb testdb --workload A --prepopulated-keys=10485760 --num-ops=1000000 --disable-wal=true --keys=zipf --duration=1200s
# ./main bench ycsb testdb --workload B --prepopulated-keys=10485760 --num-ops=1000000 --disable-wal=true --keys=zipf --duration=1200s
# ./main bench ycsb testdb --workload C --prepopulated-keys=10485760 --num-ops=1000000 --disable-wal=true --keys=zipf --duration=1200s
# ./main bench ycsb testdb --workload D --prepopulated-keys=10485760 --num-ops=1000000 --disable-wal=true --keys=zipf --duration=1200s
# ./main bench ycsb testdb --workload E --prepopulated-keys=10485760 --num-ops=1000000 --disable-wal=true --keys=zipf --duration=1200s --scans=uniform:100-100
# ./main bench ycsb testdb --workload F --prepopulated-keys=10485760 --num-ops=1000000 --disable-wal=true --keys=zipf --duration=1200s
```