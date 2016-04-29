package io.arabesque.computation

import io.arabesque.embedding.Embedding
import org.apache.spark.Logging

abstract class SparkEngine [O <: Embedding] 
    extends CommonExecutionEngine[O] with Serializable with Logging {
}
