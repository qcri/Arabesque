package io.arabesque.utils

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ObjectWritable, Writable}

class SerializableWritable[T <: Writable](@transient var t: T) extends Serializable {

  def value: T = t

  override def toString: String = t.toString

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    new ObjectWritable(t).write(out)
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    val ow = new ObjectWritable()
    ow.setConf(new Configuration(false))
    ow.readFields(in)
    t = ow.get().asInstanceOf[T]
  }

  override def hashCode = value.hashCode
  override def equals(other: Any) = other match {
    case that: SerializableWritable[_] => value.equals (that.value)
    case _ => false
  }
}
