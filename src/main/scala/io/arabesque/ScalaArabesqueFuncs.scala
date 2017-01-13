package io.arabesque

import io.arabesque.computation._
import io.arabesque.embedding._

/**
 * This is a set of aliases for extending arabesque functions using the extended
 * syntax. The most typical use for this kind of pattern is when the function
 * requires a reusable local variable in order to avoid unnecessary object
 * creation.
 *
 * TODO: provide an example
 */

trait VertexProcessFunc
    extends Function2[VertexInducedEmbedding, Computation[VertexInducedEmbedding], Unit]
    with Serializable

trait EdgeProcessFunc
    extends Function2[EdgeInducedEmbedding, Computation[EdgeInducedEmbedding], Unit]
    with Serializable
