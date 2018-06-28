title: Arabesque, A System for Distributed Graph Mining
project: Arabesque
---
# Distributed Graph Mining and Searching, Made Easy
>Arabesque is a distributed graph mining system that enables quick and easy development of graph mining algorithms, while providing a scalable and efficient implementation that runs on top of Hadoop/Spark.
>QFrag is a new distributed graph searching module in Arabesque. It also leverages the scalability and efficiency advantages introduced by Arabesque.

Benefits of Arabesque:
- Simple intuitive API, tuned for Graph Mining Problems
- Handles all the complexity of Graph Mining Algorithms transparently
- Highly Scalable
- Efficient implementation: negligible overhead compared to equivalent centralized solutions
- Support of large graphs with over a billion edges. It can process trillion of subgraphs in a commodity cluster.
- Runs as an Apache Spark or Giraph Job.

Arabesque is designed for graph mining. Graph search is a different problem than graph mining: in this case, we have a specific pattern of interest (i.e., a query graph) and we want to find all of its instances in a large data graph. QFrag is a system that implements graph search by partitioning computation, not data: every worker has a complete copy of the data graph, like in Arabesque. This enables using state-of-the-art sequential subgraph isomorphism algorithms for distributed graph search. QFrag introduces a technique called task fragmentation to balance load while preserving the sequential nature of the subgraph isomorphism algorithms it uses.

## News
- QFrag: new  distributed graph search module in Arabesque.
- The new Spark release of Arabesque is optimized to use less memory than the older versions.
- We have a new Spark 2.0 based version [download page](download.html).
- "Graph Data Mining with Arabesque", SIGMOD 2017 demo [paper](http://ds.qcri.org/publications/2017-hussein-sigmod.pdf)

## Documentation

- Check our SOSP 2015 [paper](http://ds.qcri.org/publications/2015-teixeira-sosp.pdf) that describes Arabesque's system.
- Check SoCC 2017 [paper](http://ds.qcri.org/publications/2017-serafini-socc.pdf) for more information about QFrag, our new distributed graph searching module in Arabesque.

Follow our [programming guide](user_guide.html) to learn how to program graph mining applications on Arabesque. You can then try to [run your code](how_run.html)!

## How to Use
Arabesque can be used in one of 2 ways:
* **Standalone** - If you simply want to test/execute one of the example algorithms included in Arabesque, you can simply download the JAR file from our [download page](download.html) and use the supporting execution scripts to run your desired algorithm.
* **Framework** - If you want to code your own graph mining algorithm on top of Arabesque, the easiest way to accomplish this is by forking our [Arabeque skeleton project](https://github.com/Qatar-Computing-Research-Institute/Arabesque-Skeleton) and follow the instructions in the README file.

## Source code
Arabesque is open-sourced under the Apache 2.0 license.

The source code can be accessed from [github](https://github.com/qcri/Arabesque).
