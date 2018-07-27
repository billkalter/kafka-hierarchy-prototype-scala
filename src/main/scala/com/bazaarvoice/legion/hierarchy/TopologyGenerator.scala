package com.bazaarvoice.legion.hierarchy

import com.bazaarvoice.legion.hierarchy.model._
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.{Consumed, KeyValue, StreamsBuilder, Topology}

import scala.collection.JavaConverters

object TopologyGenerator {

  def apply(config : HierarchyStreamConfig) : Topology = {
    val builder = new StreamsBuilder

    val sourceTable = builder.table(config.sourceTopic, Consumed.`with`(Serdes.String, Serdes.String))
    val parentTransitionTable = builder.table(config.parentTransitionTopic, Consumed.`with`(Serdes.String, HierarchySerdes.ParentTransition))
    val childTransitionStream = builder.stream(config.childTransitionTopic, Consumed.`with`(Serdes.String, HierarchySerdes.ChildTransition))
    val parentChildrenTable = builder.table(config.parentChildrenTopic, Consumed.`with`(Serdes.String, HierarchySerdes.ChildIdSet))
    val destTable = builder.table(config.destTopic, Consumed.`with`(Serdes.String, HierarchySerdes.Lineage))

    // Index 0 will contain all children which set themselves as their own parent
    // Index 1 will contain all otherwise valid child/parent relationships
    val splitSources: Array[KStream[String, String]] = sourceTable.toStream.branch(
      (childId: String, parentId: String) => childId == parentId,
      (_, _) => true
    )

    // Children who are their own parents are immediately undefined
    splitSources(0)
      .map((childId: String, _) => new KeyValue[String, Lineage](childId, Lineage(Reserved.UNDEFINED)))
      .to(config.destTopic, Produced.`with`(Serdes.String, HierarchySerdes.Lineage))

    splitSources(1)
      .leftJoin(parentTransitionTable, (newParentId: String, priorTransition: ParentTransition) =>
        ParentTransition(Option(priorTransition).map(t => t.newParentId).getOrElse(Reserved.UNDEFINED), newParentId))
      .filter((_, parentTransition: ParentTransition) => !parentTransition.isRedundant)
      .to(config.parentTransitionTopic, Produced.`with`(Serdes.String, HierarchySerdes.ParentTransition))

    parentTransitionTable
      .toStream
      .flatMap((childId: String, parentTransition: ParentTransition) => {
        JavaConverters asJavaIterable Seq(
          new KeyValue(parentTransition.oldParentId, ChildTransition.remove(childId)),
          new KeyValue(parentTransition.newParentId, ChildTransition.add(childId))
        )
      })
      .to(config.childTransitionTopic, Produced.`with`(Serdes.String, HierarchySerdes.ChildTransition))

    // Root parents
    childTransitionStream
      .filter((id: String, transition: ChildTransition) => id == Reserved.ROOT && transition.isAdd)
      .map((_, transition: ChildTransition) => new KeyValue(transition.childId, Lineage.empty))
      .to(config.destTopic, Produced.`with`(Serdes.String, HierarchySerdes.Lineage))

    // Parents which don't yet exist
    childTransitionStream
      .leftJoin(sourceTable, (childTransition: ChildTransition, maybeParent: String) => Option(maybeParent).isDefined)
      .filter((id: String, exists: Boolean) => !exists && id != Reserved.ROOT)
      .map((id: String, _) => new KeyValue(id, Lineage(Reserved.UNDEFINED)))
      .to(config.destTopic, Produced.`with`(Serdes.String, HierarchySerdes.Lineage))

    childTransitionStream
      .groupByKey(Serialized.`with`(Serdes.String, HierarchySerdes.ChildTransition))
      .aggregate(
        () => ChildIdSet.empty,
        (_, transition: ChildTransition, childIds: ChildIdSet) => childIds + transition,
        Materialized.`with`[String, ChildIdSet, KeyValueStore[Bytes, Array[Byte]]](Serdes.String, HierarchySerdes.ChildIdSet)
      )
      .toStream
      .to(config.parentChildrenTopic, Produced.`with`(Serdes.String, HierarchySerdes.ChildIdSet))

    parentChildrenTable
      .join(destTable, (children: ChildIdSet, lineage: Lineage) => LineageTransition(lineage, children))
      .toStream
      .flatMap((id: String, transition: LineageTransition) => {
        JavaConverters asJavaIterable (transition.parentLineage :+ id) {
            case None =>
              // Loop found in lineage.  Do not update, should probably log
              Seq.empty[KeyValue[String, Lineage]]
            case Some(updatedLineage : Lineage) =>
              transition.children.map(childId => new KeyValue(childId, updatedLineage))
          }
        })
      .to(config.destTopic, Produced.`with`(Serdes.String, HierarchySerdes.Lineage))

    builder.build
  }
}
