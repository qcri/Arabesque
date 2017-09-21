title: Arabesque, A System for Distributed Graph Mining
project: Arabesque
---
# Distributed Graph Mining, Made Easy
>Arabesque is a distributed graph mining system that enables quick and easy development of graph mining algorithms, while providing a scalable and efficient implementation that runs on top of Hadoop/Spark.

Benefits of Arabesque:
- Simple intuitive API, tuned for Graph Mining Problems
- Handles all the complexity of Graph Mining Algorithms transparently
- Highly Scalable
- Efficient implementation: negligible overhead compared to equivalent centralized solutions
- Support of large graphs with over a billion edges. It can process trillion of subgraphs in a commodity cluster.
- Runs as an Apache Spark or Giraph Job.
 
## News
- We have a new Spark 2.0 based version [download page](download.html).
- "Graph Data Mining with Arabesque", SIGMOD 2017 demo [paper](http://ds.qcri.org/publications/2017-hussein-sigmod.pdf)

## Documentation

Check our SOSP 2015 [paper](http://sigops.org/sosp/sosp15/current/2015-Monterey/printable/093-teixeira.pdf) that describes the system.

Follow our [programming guide](user_guide.html) to learn how to program graph mining applications on Arabesque. You can then try to [run your code](how_run.html)!

## How to Use
Arabesque can be used in one of 2 ways:
* **Standalone** - If you simply want to test/execute one of the example algorithms included in Arabesque, you can simply download the JAR file from our [download page](download.html) and use the supporting execution scripts to run your desired algorithm.
* **Framework** - If you want to code your own graph mining algorithm on top of Arabesque, the easiest way to accomplish this is by forking our [Arabeque skeleton project](https://github.com/Qatar-Computing-Research-Institute/Arabesque-Skeleton) and follow the instructions in the README file.

## Source code
Arabesque is open-sourced under the Apache 2.0 license.

The source code can be accessed from [github](https://github.com/Qatar-Computing-Research-Institute/Arabesque).
