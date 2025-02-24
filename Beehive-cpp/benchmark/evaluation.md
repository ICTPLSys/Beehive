# Evaluation List

## End to End Evaluation

我们的Baseline包括Fastswap和AIFM。

### Static Graph Analysis

本实验主要证明
1. 挖掘线程内的异步计算潜力很大，且可以低开销地掩盖Far Memory延迟。
2. 切换线程会导致 Locality 下降，增加Cache Miss，拖慢应用执行时间
3. 线程间Context Switch会消耗大量 CPU 周期，在Far Memory场景下比较显著

静态图分析算法，我们改写了Ligra（PPoPP'13），图包括顶点数组和顶点的邻边数组，其中邻边数组我们组织成Chunk存储作为Remotable Object存储。静态图数据生成自PBBS project并预先读入。Ligra框架将操作抽象成VertexMap和EdgeMap，按索引遍历上述数组并执行计算。因此，每个顶点上的计算是独立无依赖的，既可以线程级并行，也可以在一个线程内部异步执行。

我们有多组算法（PageRank / BFS / CC / BC等）可供评估。评估指标为End2End算法执行时间（对于PageRank，可以是每轮迭代时间）。

结果（以PageRank为例）：
1. 与All Local相比，AIFM在Far Memory（25% Local）下产生了100%的性能下降，摊销每次miss产生1.77us overhead；而我们只产生了40%的性能下降。
2. Locality下降：E2E地，AIFM增加了40%的L2 miss和30%的L3 miss，摊销每次far memory访问增加97个L2 miss和47个L3 miss，导致App（去除runtime后）执行时间增长80%（1.45us/far memory access）。而我们只增加了15%的L2和L3 miss
3. 线程调度和Context Switch消耗了可观的CPU周期，占总执行时间的8%。我们没有切换线程，按照microbenchmark的数据估算Para-routine switch开销约为0.8%，这里可以进一步profile实际值，但可能误差很大（rdtsc和routine switch开销相似）

### Key-Value Service

本实验主要证明，在延迟敏感的benchmark上，我们的细粒度、cooperative的routine切换不会产生巨大的尾延迟，反而会改善同等尾延迟目标下的吞吐。

数据结构采用
多个User Level Thread作为server执行GET, PUT, REMOVE操作（比例可调，目前90%/5%/5%）


    1. 与All local相比，25% local memory下AIFM吞吐明显下降到一半。Evacuation占17%的执行时间。对比之下，我们的实现中吞吐下降？，Evacuation占?的比例
    2. Evacuation中swap out导致频繁的线程切换，我们采用了异步的网络，Evacuation中途Context Switch少?

