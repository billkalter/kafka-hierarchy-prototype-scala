package com.bazaarvoice.legion.hierarchy

import java.io._
import java.util

import com.bazaarvoice.legion.hierarchy.model._
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

object HierarchySerdes {

  val ChildIdSet : Serde[ChildIdSet] = Serdes.serdeFrom( new ModelSerializer[ChildIdSet](), new ModelDeserializer[ChildIdSet])
  val ChildTransition : Serde[ChildTransition] = Serdes.serdeFrom( new ModelSerializer[ChildTransition](), new ModelDeserializer[ChildTransition])
  val ParentTransition : Serde[ParentTransition] = Serdes.serdeFrom( new ModelSerializer[ParentTransition](), new ModelDeserializer[ParentTransition])
  val Lineage : Serde[Lineage] = Serdes.serdeFrom( new ModelSerializer[Lineage](), new ModelDeserializer[Lineage])
  val LineageTransition : Serde[LineageTransition] = Serdes.serdeFrom( new ModelSerializer[LineageTransition](), new ModelDeserializer[LineageTransition])

  private class ModelSerializer[M : Serializable] extends Serializer[M] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, data: M): Array[Byte] = {
        val bout = new ByteArrayOutputStream
        val out = new ObjectOutputStream(bout)
        try
          out.writeObject(data)
        finally out.close()
        bout.toByteArray
      }

    override def close(): Unit = {}
  }

  private class ModelDeserializer[M : Serializable] extends Deserializer[M] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def deserialize(topic: String, data: Array[Byte]): M = {
        val in = new ObjectInputStream(new ByteArrayInputStream(data))
        try
          val obj : Any = in.readObject
          obj.asInstanceOf[M]
        finally in.close()
      }

    override def close(): Unit = {}
  }
}
