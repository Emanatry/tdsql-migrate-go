# benchmarks
## sortdata

测试 c++ 与 go 对数据集进行排序所使用的时间和内存

构建：`./make.sh`
运行：`./run.sh <csv数据文件>`，如：`../../data/src_a/a/1.csv`

### 样例输出

```
c++: gcc 11.2.0 -O2
go: go1.17
os: linux 5.13.0 x64
cpu: Intel(R) Core(TM) i3-8100 CPU @ 3.60GHz
```

```
==== cppver ====
read: 240ms
sort: 41ms
write: 314ms
mem=0KB RSS=81260KB elapsed=0:00.61 cpu.sys=0.07 user=0.35
==== gover ====
read: 227ms
sort: 117ms
write: 127ms
mem=0KB RSS=156824KB elapsed=0:00.48 cpu.sys=0.09 user=0.48
```