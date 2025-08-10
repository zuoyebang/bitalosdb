# Benchmark

这个benchmark将测试BitalosDB的随机读写性能。

## Build

```
cd bench/dbbench
./make
```

## Run benchmark

```
# Note: delete db
mkdir log
num=335544320
vlen=128
sz=40g
dbname=40g128
# First test the write performance
./main -command randomWrite -dbname {dbname} -max ${num} -db 1 -gonum 4 -cpus 4 -valueType 1 -vlen ${vlen} -delayExit 8 > log/${sz}_4c_${vlen}_${num}-write.log 2>&1 &
# Secondly test the read performance
./main -db 1 -command r -dbname {dbname} -gonum 4 -cpus 4 -max ${num} -delayExit 30 > log/${sz}_4c_${vlen}_${num}-read.log 2>&1 &
```