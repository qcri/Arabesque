# Arabesque: Distributed graph mining made simple

[http://arabesque.io](http://arabesque.io)

*Current Version:* 1.0-BETA

Arabesque is a distributed graph mining system that enables quick and easy
development of graph mining algorithms, while providing a scalable and efficient
execution engine running on top of Hadoop.

Benefits of Arabesque:
* Simple and intuitive API, specially tailored for Graph Mining algorithms.
* Transparently handling of all complexities associated with these algorithms.
* Scalable to hundreds of workers.
* Efficient implementation: negligible overhead compared to equivalent centralized solutions.

Arabesque is open-source with the Apache 2.0 license.

## Requirements for running

* Linux/Mac with 64-bit JVM
* [A functioning installation of Hadoop2 with MapReduce (local or in a cluster)](http://www.alexjf.net/blog/distributed-systems/hadoop-yarn-installation-definitive-guide/)

## Preparing your input
Arabesque currently takes as input graphs with the following format:

```
# <num vertices> <num edges>
<vertex id> <vertex label> [<neighbour id1> <neighbour id2> ... <neighbour id n>]
<vertex id> <vertex label> [<neighbour id1> <neighbour id2> ... <neighbour id n>]
...
```

Vertex ids are expected to be sequential integers between 0 and (total number of vertices - 1).

## Test/Execute the included algorithms

You can find an execution-helper script and several configuration files for the different algorithms under the [scripts
folder in the repository](https://github.com/Qatar-Computing-Research-Institute/Arabesque/tree/master/scripts):

* `run_arabesque.sh` - Launcher for arabesque executions. Takes as parameters one or more yaml files describing the configuration of the execution to be run. Configurations are applied in sequence with configurations in subsequent yaml files overriding entries of previous ones.
* `cluster.yaml` - File with configurations related to the cluster and, so, common to all algorithms: number of workers, number of threads per worker, number of partitions, etc.
* `<algorithm>.yaml` - Files with configurations related to particular algorithm executions using as input the [provided citeseer graph](https://github.com/Qatar-Computing-Research-Institute/Arabesque/tree/master/data):
  * `fsm.yaml` - Run frequent subgraph mining over the citeseer graph.
  * `cliques.yaml` - Run clique finding over the citeseer graph.
  * `motifs.yaml` - Run motif counting over the citeseer graph.
  * `triangles.yaml` - Run triangle counting over the citeseer graph.

**Steps:**

1. Compile Arabesque using 
  ```
  mvn package
  ```
  You will find the jar file under `target/`
  
2. Copy the newly generated jar file, the `run_arabesque.sh` script and the desired yaml files onto a folder on a computer with access to an Hadoop cluster. 

3. Upload the input graph to HDFS. Sample graphs are under the `data` directory. Make sure you have initialized HDFS first.

  ```
  hdfs dfs -put <input graph file> <destination graph file in HDFS>
  ```

4. Configure the `cluster.yaml` file with the desired number of containers, threads per container and other cluster-wide configurations.

5. Configure the algorithm-specific yamls to reflect the HDFS location of your input graph as well as the parameters you want to use (max size for motifs and cliques or support for FSM).

6. Run your desired algorithm by executing:

  ```
  ./run_arabesque.sh cluster.yaml <algorithm>.yaml
  ```

7. Follow execution progress by checking the logs of the Hadoop containers.

8. Check any output (generated with calls to the `output` function) in the HDFS path indicated by the `output_path` configuration entry.


## Implementing your own algorithms
The easiest way to get to code your own implementations on top of Arabesque is by forking our [Arabesque Skeleton Project](https://github.com/Qatar-Computing-Research-Institute/Arabesque-Skeleton). You can do this via
[Github](https://help.github.com/articles/fork-a-repo/) or manually by executing the following:

```
git clone https://github.com/Qatar-Computing-Research-Institute/Arabesque-Skeleton.git $PROJECT_PATH
cd $PROJECT_PATH
git remote rename origin upstream
git remote add origin $YOUR_REPO_URL
```
