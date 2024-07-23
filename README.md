![bitalos](./docs/bitalos.png)

### Bitalosdb is a high-performance KV storage engine. [中文版](./README_CN.md)

## Introduction

- Bitalosdb is a high-performance KV storage engine (self-developed), which is based on the creative IO architecture and storage technology and focuses on solving the problem of LSM-Tree read and write amplification. As a substitute for Rocksdb, Bitalosdb can improve read and write performance greatly.

## Team

- Produced: Zuoyebang Company - Platform technical team

- Author: Xu Ruibo(hustxurb@163.com)

- Contributors: Xing Fu(wzxingfu@gmail.com), Lu Wenwei(422213023@qq.com), Liu Fang(killcode13@sina.com)

## Key Technology

- Bithash (KV separation technology), significantly reduce write amplification. For bithash, time complexity of retrieval is O(1). GC can be completed independently, and value and index are decoupled.

- Bitalostree (high-performance compression index technology), basically eliminate read amplification. If B+ Tree has a number of huge Pages, write amplification is a severe problem. With a creative index compression technology, Bitalostree eliminates B+ Tree write amplification, and improves the read performance.

- Bitalostable (cold and hot data separation technology), stores cold data, which is calculated according to the data scale and access frequency. Storage engine writes cold data to Bitalostable when QPS becomes low. Improve data compression, reduce index memory consumption, and achieve more rational resource utilization. (open source stable edition has basic features, enterprise edition supports more comprehensive hot and cold separation).

## Performance

- This benchmark is based on Bitloasdb version (v5.0) and rocksdb stable version (v7.6.0).

### Hardware

```
CPU:    Intel(R) Xeon(R) Platinum 8255C CPU @ 2.50GHz
Memory: 384GB
Disk:   2*3.5TB NVMe SSD
```

- IOPS&BW (File)

|BlockSize| Write | RandWrite | Read | RandRead 
|---------|----------|----------|------|-----
| 4KB  | 294K(1150MiB/s)  | 232K(905MiB/s) | 446K(1742MiB/s) | 446K(1743MiB/s) 
| 8KB | 266K(2080MiB/s) | 244K(1902MiB/s) | 309K(2414MiB/s) | 404K(3159MiB/s)

### Program

- Benchmark thread number: 8

- Program cpu cgroup: 8 core

- Comparison standard: QPS on single-core (multi-core QPS / core number), single-core performance reflects cost advantage better.

### Data

- Key-value spec: key-size=32B、value-size=1KB

- Comparison dimensions: Total data size(25/50/100GB) * IO ratio(100% random write, 100% random read, 50% random write + 50% random read, 30% random write + 70% random read)

### Config

- rocksdb

```
Memtable：256MB
WAL：enable
Cache：8GB
TargetFileSize：256M
L0CompactTrigger：8
L0StopWritesTrigger：24
```

- bitalosdb

```
Memtable：256MB
WAL：enable
Cache：disable
```

### Result

- QPS ([Horizontal](./docs/benchmark-qps.png))

![benchmark](./docs/benchmark-qps-vertical.png)

## Document

Technical architecture and documentation, refer to the official website: [bitalos.zuoyebang.com](https://bitalos.zuoyebang.com)