package io.arabesque

import org.apache.spark.SparkContext

/**
  * Created by ehussein on 7/23/17.
  */
class EmbeddingsValidator(sc: SparkContext) {

  def validate(groundTruthPath: String, resultsPath: String, embeddingType: String): Boolean = {

    val embeddings1 = sc.textFile(groundTruthPath).
      map(line => line.split(" "))

    val embeddings2 = sc.textFile(resultsPath).
      map(line => line.split(" "))

    val emb1Count = embeddings1.count()
    val emb2Count = embeddings2.count()
    println(s"emb1Count = $emb1Count")
    println(s"emb2Count = $emb2Count")

    if(emb1Count != emb2Count) {
      println(s"Different number of embeddings, where count(first_embeddings) = $emb1Count " +
        s"and count(second_embeddings) = $emb2Count")
      return false
    }

    if(embeddingType.equalsIgnoreCase("VertexInduced")) {
      val emb1Sorted = embeddings1.map(x => x.map(id => id.toInt)).collect().sorted(VertexInducedEmbeddingsOrdering)
      val emb2Sorted = embeddings2.map(x => x.map(id => id.toInt)).collect().sorted(VertexInducedEmbeddingsOrdering)

      var i = 0

      while(i < emb1Count) {
        if(compareEmbeddings(emb1Sorted(i), emb2Sorted(i)) != 0) {
          println(s"\n\nResults are not the same:")
          print("\nemb1 = ")
          emb1Sorted(i).foreach(x => print(s"$x "))
          print("\nemb2 = ")
          emb2Sorted(i).foreach(x => print(s"$x "))
          return false
        }

        i += 1
      }
    }
    else if(embeddingType.equalsIgnoreCase("EdgeInduced")) { // EdgeInduce
      val emb1Sorted = embeddings1.collect().sorted(EdgeInducedEmbeddingsOrdering)
      val emb2Sorted = embeddings2.collect().sorted(EdgeInducedEmbeddingsOrdering)
      var i = 0
      while(i < emb1Count) {
        if(compareEmbeddings(emb1Sorted(i), emb2Sorted(i)) != 0) {
          println(s"\n\nResults are not the same:")
          print("\nemb1 = ")
          emb1Sorted(i).foreach(x => print(s"$x "))
          print("\nemb2 = ")
          emb2Sorted(i).foreach(x => print(s"$x "))
          return false
        }
        i += 1
      }
    }
    else {
      println("Unknown embedding type: " + embeddingType)
      return false
    }

    println("\n###*@!!!@*###       The resulting embeddings are identical.       ###*@!!!@*###\n")

    true
  }

  object VertexInducedEmbeddingsOrdering extends Ordering[Array[Int]] {
    def compare(a:Array[Int], b:Array[Int]) = compareEmbeddings(a, b)
  }

  object EdgeInducedEmbeddingsOrdering extends Ordering[Array[String]] {
    def compare(a:Array[String], b:Array[String]): Int = compareEmbeddings(a, b)
  }

  def compareEmbeddings(e1: Array[Int], e2: Array[Int]): Int = {
    val e1Count = e1.length
    val e2Count = e2.length

    if(e1Count != e2Count)
      return e1Count - e2Count

    var i = 0
    var result: Int = 0
    while(i < e1Count) {
      if(e1(i) < e2(i)) {
        result = -1
        i = e1Count + 1
      }
      else if(e1(i) > e2(i)) {
        result = 1
        i = e1Count + 1
      }
      i += 1
    }

    result
  }

  def compareEmbeddings(e1: Array[String], e2: Array[String]): Int = {
    val e1Count = e1.length
    val e2Count = e2.length

    if(e1Count != e2Count)
      return e1Count - e2Count

    var i = 0
    var result: Int = 0
    while(i < e1Count) {
      if(e1(i) < e2(i)) {
        result = -1
        i = e1Count + 1
      }
      else if(e1(i) > e2(i)) {
        result = 1
        i = e1Count + 1
      }
      i += 1
    }

    result
  }
}
