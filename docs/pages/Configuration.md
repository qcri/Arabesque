# Configuration paramters

This document describes the paramaters used to configure Arabesque and QFrag. It also describes the parameters used to configure a spark cluster


***


##  Cluster:
| Parameter | Description | Default value |
| ------------------- | -------------- | :--------------: |
| **`spark_master`** | This property determines the deploy mode of the application. [Allowed values](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls) are the same as in any Spark application. | `local[*]` |
| **`num_workers`** | Number of Spark executors requested by the application. | `1` |
| **`num_compute_threads`** | Number of cores per executor requested by the application | `1` |
| **`num_partitions`** | The number of parallel execution engines used in Arabesque. It should not be less than the number of cores available in the cluster. | [SparkContext<br>.defaultParallelism](https://spark.apache.org/docs/latest/configuration.html#execution-behavior) |
| **`driver_memory`** | Sets the desired memory for the driver | `1g` |
| **`executor_memory`** | Sets the desired memory for each executor | `1g` |
| **`max_result_size`** | Determines the maximum size of the serialized results that will be returned to the driver from each task (Note: this value should be less than the driver memory or it will cause OOM errors) | `1g` |


***


##  Common paramters:
| Parameter | Description | Default value |
| ------------------- | -------------- | :--------------: |
| **`system_type`** | Determines the system: either **`mining`** to run Arabesque or **`search`** to run QFrag | `mining` |
| **`log_level`** | Determines the log level of the application, you can choose one of the [log4j logging levels]() | `info` |


***


##  Arabesque:
| Parameter | Description | Default value |
| ------------------- | -------------- | :--------------: |
| **`execution_engine`** | Determines whether Arabesque will run on top of Giraph or Spark. User must determines a value, either `giraph` or `spark` | No default value |
| **`input_graph_path`** | Determines the location/path of the input main graph | `main.graph` |
| **`input_graph_subgraphs_path`** | Determines the location/path of the input subgraphs file | `None` |
| **`input_graph_local`** | Determines whether the data graph is stored on the local storage or on hdfs | `false` |
| **`output_path`** | Tells Arabesque where to write the results (patterns and/or embeddings) if `output_active` is set to true | `Output` |
| **`output_active`** | Tells Arabesque whether to write the results (patterns and/or embeddings) into `output_path` or not | `true` |
| **`computation`** | Specify the mining algorithm computation class. According to the current implemented algorithms available under the `gmlib` package, this parameter can take one of the following values:<ul><li>`io.arabesque.gmlib.clique.CliqueComputation`</li><li>`io.arabesque.gmlib.fsm.FSMComputation`</li><li>`io.arabesque.gmlib.motif.MotifComputation`</li></ul> | `io.arabesque`<br>`.computation`<br>`.ComputationContainer` |
| **`master_computation`** | Specify the class of the master computation that would be executed before the end of each super step. Example: io.arabesque.computation.FSMMasterComputation prints the frequent patterns or halts the computation if no patterns survived the fiteration step | `io.arabesque`<br>`.computation`<br>`.MasterComputation` |
| **`arabesque.clique.maxsize`** | Arabesque will continue mining all cliques with size <= `arabesque.clique.maxsize` | `4` |
| **`arabesque.motif.maxsize`** | Arabesque will continue mining/counting all motifs with size <= `arabesque.motif.maxsize` | `4` |
| **`arabesque.fsm.maxsize`** | Arabesque will continue mining till it reaches patterns with size =  `arabesque.fsm.maxsize` and support >= `arabesque.fsm.support` | `4` |
| **`arabesque.fsm.support`** | Arabesque will continue mining till it reaches patterns with size =  `arabesque.fsm.maxsize` and support >= `arabesque.fsm.support` | `Integer.MAX_VALUE` |
| **`comm_strategy`** | The communication strategy used to re-distribute the embedding on each superstep. The following values are currently supported: <ul><li><code>odag_sp</code> or <code>odag_sp_primitive</code>: Single pattern primitive ODAGs are used to pack embeddings in an space-efficient structure</li><li><code>odag_sp_generic</code>: Single pattern generic ODAGs are used to pack embeddings in an space-efficient structure</li><li><code>embedding</code>: the embedding are packed with a common compression algorithm (LZ4).</li></ul> | `odag_sp` |
| **`flush_method`** | This property is required when `comm_strategy` is `odag_sp`. In particular, we aggregate the ODAGs according to one of the following criteria: <ul><li><code>flush_by_pattern</code>: patterns are used as aggregation key. This is a good alternative when the number of instance per pattern is roughly uniform, which is very rare.</li><li><code>flush_by_entry</code>: every entry (pattern,domainId,wordId) in the ODAG is used as a composite key for aggregation. This is efficient when the distribution of instances per pattern is irregular but the number of domains is small.</li><li><code>flush_by_parts</code>: ranges of domains in the ODAG are used as key for aggregation. This is efficient for irregular distributions of instances among patterns.</li></ul> | `flush_by_parts` |
| **`num_odag_parts`** | The number of parts used to split the ODAG for aggregation when the communication strategy is `odag_sp` and the flush method is `flush_by_parts` | `-1` |
| **`arabesque.aggregators`<br>`.default_splits`** | Split all aggregations in parts for parallel aggregation. *use only with heavy aggregations*. | `1` |
| **`incremental_aggregation`** | Merges or replaces the aggregations for the next superstep. We can have one of the following scenarios:<ul><li> `true`: In any superstep we are interested in all aggregations seen so far. Thus, the aggregations are incrementally composed </li><li> `false`: In any superstep we are interested only in the previous aggregations. Thus, we discard the old aggregations and replace it with the new aggregations for the next superstep </li></ul> | `false` |


***


##  QFrag:
| Parameter | Description | Default value |
| ------------------- | -------------- | :--------------: |
| **`search_input_graph_path`** | Determines the location/path of the data graph. This graph is where QFrag will search for <br>matches of the `search_query_graph_path`  | `null` |
| **`search_query_graph_path`** | Determines the location/path of the query/pattern graph | `null` |
| **`search_output_path`** | Tells QFrag where to write the resulting matches if `search_output_active` is set to true | `output_search` |
| **`search_output_active`** | Tells QFrag whether to to write the resulting matches into `search_output_path` | `true` |
| **`search_injective`** | True if the mapping between query vertices and data vertices is injective, i.e., the same data <br>vertex cannot map to two different query vertices | `false` |
| **`search_write_in_binary`** | Determines if the output should be written in binary or text  | `false` |
| **`search_num_vertices`** | Number of vertices in the input graph
 | `-1` |
| **`search_num_edges`** | Number of edges in the input graph | `-1` |
| **`search_num_labels`** | Number of labels in the input graph | `-1` |
| **`search_multi_vertex_labels`** | True if vertices in the input graph can have multiple labels (experimental, not tested thoroughly)  | `false` |
