package io.arabesque.computation

import io.arabesque.embedding.Embedding
import io.arabesque.utils.Logging

trait SparkEngine [O <: Embedding] 
    extends CommonExecutionEngine[O] with Serializable with Logging {

    var computed = false
}
