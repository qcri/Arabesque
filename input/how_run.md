title: How to run Arabesque
project: Arabesque
---
# How to Run an Arabesque Job

## Requirements

* Linux/Mac with 64-bit JVM
* [A functioning installation of Hadoop2 with MapReduce (local or in a cluster)](http://www.alexjf.net/blog/distributed-systems/hadoop-yarn-installation-definitive-guide/)

## Helper scripts and configuration files
You can find an execution-helper script and several configuration files for the different algorithms under the [scripts
folder in the repository](https://github.com/Qatar-Computing-Research-Institute/Arabesque/tree/master/scripts):

* `run_arabesque.sh` - Launcher for arabesque executions. Takes as parameters one or more yaml files describing the configuration of the execution to be run. Configurations are applied in sequence with configurations in subsequent yaml files overriding entries of previous ones.
* `cluster.yaml` - File with configurations related to the cluster and, so, common to all algorithms: number of workers, number of threads per worker, number of partitions, etc.
* `<algorithm>.yaml` - Files with configurations related to particular algorithm executions using as input the [provided citeseer graph](https://github.com/Qatar-Computing-Research-Institute/Arabesque/tree/master/data):
  * `fsm.yaml` - Run frequent subgraph mining over the citeseer graph.
  * `cliques.yaml` - Run clique finding over the citeseer graph.
  * `motifs.yaml` - Run motif counting over the citeseer graph.
  * `triangles.yaml` - Run motif counting over the citeseer graph.


## Steps
1. Put the Arabesque jar, the `run_arabesque.sh` script and desired yaml files in a folder on a computer with access to an Hadoop cluster. 

2. Upload the input graph to HDFS.  Sample graphs are under the `data` directory. Make sure you have initialized HDFS first.

  ```
  hdfs dfs -put <input graph file> <destination graph file in HDFS>
  ```

3. Configure the `cluster.yaml` file with the desired number of containers, threads per container and other cluster-wide configurations.

4. Configure the algorithm-specific yamls to reflect the HDFS location of your input graph as well as the parameters you want to use (max size for motifs and cliques or support for FSM).

5. Run your desired algorithm by executing:

  ```
  ./run_arabesque.sh cluster.yaml <algorithm>.yaml
  ```

6. Follow execution progress by checking the logs of the Hadoop containers.

7. Check any output (generated with calls to the `output` function) in the HDFS path indicated by the `output_path` configuration entry.

## Extra Parameters
* **communication_strategy** - Dictates whether to use the ODAGs (default, corresponding to the `odag` value) or a simple compressed embedding list to store the embeddings (corresponding to the `embeddings` value). 

  Simple lists can be beneficial when we consider shallow depths (<=3) or very restricted explorations (small number of embeddings) where the potential savings for ODAGs aren't high and their construction would just incur in extra overhead. In most cases though, ODAGs are vastly superior. 

* **arabesque.aggregators.default_splits** - In how many parts to split aggregated values (default=1).

  Heavy aggregations handling thousands of different keys might benefit from being split into several parts to speedup execution and network communication. However, splitting simple aggregations will add unnecessary overhead.

## Requirements for Input Graph
Arabesque currently takes as input graphs with the following format:

```
# <num vertices> <num edges>
<vertex id> <vertex label> [<neighbour id1> <neighbour id2> ... <neighbour id n>]
<vertex id> <vertex label> [<neighbour id1> <neighbour id2> ... <neighbour id n>]
...
```

Vertex ids are expected to be sequential integers between 0 and (total number of vertices - 1).

You can examine our sample citeseer graphs [here](https://github.com/Qatar-Computing-Research-Institute/Arabesque/tree/master/data).

# Developing your own algorithms
The easiest way to start developing your own graph mining algorithms on top of Arabesque is to clone our [Arabesque-Skeleton repository](https://github.com/Qatar-Computing-Research-Institute/Arabesque-Skeleton). This gives you a preconfigured Maven project with detailed instructions on how to get your code running in a cluster.