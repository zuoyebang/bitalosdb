![bitalos](./docs/bitalos.png)

## 简介

- 高性能KV存储引擎（自研），基于全新IO架构及存储技术，重点解决LSM-Tree的读写放大问题。作为rocksdb的替代品，读写性能均有大幅提升。

## 出品

- 团队：作业帮-平台技术团队

- 作者：徐锐波(hustxurb@163.com)

- 贡献者：幸福(wzxingfu@gmail.com)、卢文伟(422213023@qq.com)、刘方(killcode13@sina.com)

## 关键技术
- Bithash（KV分离技术），显著改善写放大；具备O(1)检索效率，可独立完成GC，实现value与index解耦。

- Bitalostree（高性能压缩索引技术），基本消除读放大；基于超大Page的B+ Tree，运用全新的索引压缩技术，消除B+ Tree的写放大，并将读性能发挥到极致。

- Bitalostable（冷热数据分离技术），承载冷数据存储，根据数据规模及访问频度，计算相对冷数据，流量低峰时转存至Bitalostable；提升数据压缩率，减少索引内存消耗，实现更合理的资源利用（开源稳定版具备基础功能，企业版支持更全面的冷热分离）。

## 性能报告

- bitalosdb在性能上持续精进，此次性能测试基于bitalosdb v5.0；作为rocksdb的替代品，选取同时期rocksdb稳定版做性能对比。

### 硬件

```
CPU:    Intel(R) Xeon(R) Platinum 8255C CPU @ 2.50GHz
Memory: 384GB
Disk:   2*3.5TB SSD
```

- Disk(File) IOPS(BW)

|BlockSize| Write | RandWrite | Read | RandRead 
|---------|----------|----------|------|-----
| 4KB  | 294K(1150MiB/s)  | 232K(905MiB/s) | 446K(1742MiB/s) | 446K(1743MiB/s) 
| 8KB | 266K(2080MiB/s) | 244K(1902MiB/s) | 309K(2414MiB/s) | 404K(3159MiB/s)

### 程序

- 压测线程：8

- CPU限制：8核

- 对比标准：多核压测QPS换算成单核QPS对比，单核性能更能体现成本优势

### 数据

- 单条数据：key-size：32B、value-size：1KB

- 对比维度：数据总量（25GB、50GB、100GB） x 读写占比（100%随机写、100%随机读、50%随机写+50%随机读、30%随机写+70%随机读）

### 配置

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

### 结果

- QPS ([Horizontal](./docs/benchmark-qps.png))

![benchmark](./docs/benchmark-qps-vertical.png)

## 文档

- 技术架构及文档，参考官网：[bitalos.zuoyebang.com](https://bitalos.zuoyebang.com)