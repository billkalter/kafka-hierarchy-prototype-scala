package com.bazaarvoice.legion.hierarchy

import java.io._
import java.util

import com.bazaarvoice.legion.hierarchy.model._
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.kstream.{Joined, Materialized, Produced, Serialized}
import org.apache.kafka.streams.state.KeyValueStore

object HierarchySerdes {

  class ModelSerializer[M <: Serializable] extends Serializer[M] {
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

  class ModelDeserializer[M <: Serializable] extends Deserializer[M] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

    override def deserialize(topic: String, data: Array[Byte]): M = {
        val in = new ObjectInputStream(new ByteArrayInputStream(data))
        try {
          val obj: Any = in.readObject
          obj.asInstanceOf[M]
        } finally in.close()
      }

    override def close(): Unit = {}
  }

  // This is the one type exposed in the final result topic, so it needs a resolvable type
  class LineageDeserializer extends ModelDeserializer[Lineage]

  // Implicits used for KStreams

  implicit val childIdSetSerde : Serde[ChildIdSet] = Serdes.serdeFrom( new ModelSerializer[ChildIdSet](), new ModelDeserializer[ChildIdSet])
  implicit val childTransitionSerde : Serde[ChildTransition] = Serdes.serdeFrom( new ModelSerializer[ChildTransition](), new ModelDeserializer[ChildTransition])
  implicit val parentTransitionSerde : Serde[ParentTransition] = Serdes.serdeFrom( new ModelSerializer[ParentTransition](), new ModelDeserializer[ParentTransition])
  implicit val lineageSerde : Serde[Lineage] = Serdes.serdeFrom( new ModelSerializer[Lineage](), new LineageDeserializer)
  implicit val lineageTransitionSerde : Serde[LineageTransition] = Serdes.serdeFrom( new ModelSerializer[LineageTransition](), new ModelDeserializer[LineageTransition])

  implicit val consumedStringString : Consumed[String, String] = Consumed.`with`(Serdes.String, Serdes.String)
  implicit val consumedStringParentTransition : Consumed[String, ParentTransition] = Consumed.`with`(Serdes.String, parentTransitionSerde)
  implicit val consumedStringChildTransition : Consumed[String, ChildTransition] = Consumed.`with`(Serdes.String, childTransitionSerde)
  implicit val consumedStringChildIdSet : Consumed[String, ChildIdSet] = Consumed.`with`(Serdes.String, childIdSetSerde)
  implicit val consumedStringLineage : Consumed[String, Lineage] = Consumed.`with`(Serdes.String, lineageSerde)

  implicit val producedStringParentTransition : Produced[String, ParentTransition] = Produced.`with`(Serdes.String, parentTransitionSerde)
  implicit val producedStringChildTransition : Produced[String, ChildTransition] = Produced.`with`(Serdes.String, childTransitionSerde)
  implicit val producedStringChildIdSet : Produced[String, ChildIdSet] = Produced.`with`(Serdes.String, childIdSetSerde)
  implicit val producedStringLineage : Produced[String, Lineage] = Produced.`with`(Serdes.String, lineageSerde)

  implicit val joinedStringStringParentTransition : Joined[String, String, ParentTransition] = Joined.`with`(Serdes.String, Serdes.String, parentTransitionSerde)
  implicit val joinedStringChildTransitionString : Joined[String, ChildTransition, String] = Joined.`with`(Serdes.String, childTransitionSerde, Serdes.String)
  implicit val joinedStringLineageLineage : Joined[String, Lineage, Lineage] = Joined.`with`(Serdes.String, lineageSerde, lineageSerde)

  implicit val stringChildTransitionSerialized : Serialized[String, ChildTransition] = Serialized.`with`(Serdes.String, childTransitionSerde)
  implicit val materializedParentChildren : Materialized[String, ChildIdSet, KeyValueStore[Bytes, Array[Byte]]] = Materialized.`with`(Serdes.String, childIdSetSerde)
}
