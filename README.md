[![Build Status](https://travis-ci.org/dccspeed/Arabesque.svg?branch=master)](https://travis-ci.org/dccspeed/Arabesque)

# Arabesque: Distributed graph mining made simple

[http://arabesque.io](http://arabesque.io)

*Current Version:* 1.0.0-SPARK

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
* At least one of the supported execution engines installed (local or in a cluster):
   * [Hadoop2/Giraph](http://www.alexjf.net/blog/distributed-systems/hadoop-yarn-installation-definitive-guide/) or
   * [Spark](https://chongyaorobin.wordpress.com/2015/07/01/step-by-step-of-installing-apache-spark-on-apache-hadoop/)


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
* `cluster.yaml` - File with configurations related to the cluster and, so, common to all algorithms: execution engine, number of workers, number of threads per worker, number of partitions, etc.
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

4. Configure the `cluster.yaml` file with the desired number of containers, threads per container and other cluster-wide configurations. Remember to include the desirable execution engine: (i) 'spark' for [Spark](http://spark.apache.org/) or 'giraph' [Hadoop2/Giraph](http://giraph.apache.org/). In case of Spark, include also the [master URL](http://spark.apache.org/docs/latest/submitting-applications.html#master-urls) (`spark_master`) which indicates the desirable deployment. See folder `scripts` for examples.

5. Configure the algorithm-specific yamls to reflect the HDFS location of your input graph as well as the parameters you want to use (max size for motifs and cliques or support for FSM).

6. Run your desired algorithm by executing:

  ```
  # giraph
  ./run_arabesque.sh cluster-giraph.yaml <algorithm>.yaml
  # or spark (make sure SPARK_HOME is set)
  ./run_arabesque.sh cluster-spark.yaml <algorithm>.yaml
  ```

7. Follow execution progress by checking the logs of the Hadoop containers or
   local logs.

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

## Arabesque notebook

Arabesque notebook runs in [Toree](https://github.com/apache/incubator-toree),
which deploy Spark in Jupyter notebook environment. Follow these steps:
   1. Configure the env variables for Spark and Arabesque:

   ```
   export SPARK_HOME=<path_to_spark_installation>
   export ARABESQUE_HOME=<path_to_arabesque_installation>
   ```

   You can do it by setting ```arabesque-env.sh``` (root of the project) and sourcing it:

   ```
   source arabesque-env.sh
   ```

   2. [Install Jupyter](http://jupyter.readthedocs.org/en/latest/install.html) 

   3. Install toree using pip installer:

   ```
   pip install toree
   ```

   4. Register toree kernel with jupyter:

   ```
   jupyter toree install \
      --spark_home=$SPARK_HOME \
      --spark_opts="--master local[*] --jars target/arabesque-1.0.2-BETA-jar-with-dependencies.jar" \
      --kernel_name="arabesque_1.0.2" \
      --user
   ```

   5. Start jupyter notebook:

   ```
   cd $ARABESQUE_HOME
   jupyter notebook
   ```

   6. The last step will open jupyter's web interface. Just click on
      ```arabesque-demo.ipynb``` and try some concepts/snippets.

## Embedding communication strategies for Spark

The system supports the following communication strategies in Spark mode:

* `odag`: the produced embeddings from each superstep are distributed in a
  compressed structure called ODAG (refer to the paper for details). This mode
  is activated with the following property (included in yaml files or Scala API):
  * key: `comm_strategy`; value: `odag`
* `embedding`: the produced embeddings are packed together and distributed over
  the network. In this mode we do not leverage graph properties for compressing,
  however we gain in access time if memory is not an issue. This mode is
  activated with the following property (included in yaml files or Scala API):
  * key: `comm_strategy`; value: `embedding`
