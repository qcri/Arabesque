#### Spark-Arabesque Properties

The following properties assume that the execution engine selected for the application is Spark, i.e., the property `execution_engine` is `spark`.

| Property Name | Meaning | Default |
| :------------ | :------ | :------ |
| `spark_master` | This property determines the deploy mode of the application. [Allowed values](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls) are the same as in any Spark application. | `local[*]` |
| `num_workers` | Number of Spark executors requested by the application. | 1 |
| `num_compute_threads` | Number of cores per executor requested by the application | 1 |
| `num_partitions` | The number of parallel execution engines used in Arabesque. It should not be less than the number of cores available in the cluster. | [default parallelism](https://spark.apache.org/docs/latest/configuration.html#execution-behavior) in the SparkContext |
| `comm_strategy` | The communication strategy used to re-distribute the embedding on each superstep. The following values are currently supported: <ul><li><code>odag_sp</code>: ODAGs are used to pack embeddings in an space-efficient structure</li><li><code>embedding</code>: the embedding are packed with a common compression algorithm (LZ4).</li></ul> | `odag_sp` |
| `flush_method` | This property is required when `comm_strategy` is `odag_sp`. In particular, we aggregate the ODAGs according to one of the following criteria: <ul><li><code>flush_by_pattern</code>: patterns are used as aggregation key. This is a good alternative when the number of instance per pattern is roughly uniform, which is very rare.</li><li><code>flush_by_entry</code>: every entry (pattern,domainId,wordId) in the ODAG is used as a composite key for aggregation. This is efficient when the distribution of instances per pattern is irregular but the number of domains is small.</li><li><code>flush_by_parts</code>: ranges of domains in the ODAG are used as key for aggregation. This is efficient for irregular distributions of instances among patterns.</li></ul> | `flush_by_parts` |
| `num_odag_parts` | The number of parts used to split the ODAG for aggregation when the communication strategy is `odag_sp` and the flush method is `flush_by_parts` | `num_partitions` |
