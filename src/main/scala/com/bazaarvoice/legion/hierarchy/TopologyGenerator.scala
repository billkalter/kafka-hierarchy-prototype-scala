package com.bazaarvoice.legion.hierarchy

import com.bazaarvoice.legion.hierarchy.HierarchySerdes._
import com.bazaarvoice.legion.hierarchy.model._
import com.lightbend.kafka.scala.streams.StreamsBuilderS
import org.apache.kafka.streams.Topology
import org.slf4j.{Logger, LoggerFactory}

object TopologyGenerator {

  val _log : Logger = LoggerFactory.getLogger(TopologyGenerator.getClass)

  def apply(config : HierarchyStreamConfig) : Topology = {
    val builder = new StreamsBuilderS()

    val sourceTable = builder.table[String, String](config.sourceTopic)
    val parentTransitionTable = builder.table[String, ParentTransition](config.parentTransitionTopic)
    val childTransitionStream = builder.stream[String, ChildTransition](config.childTransitionTopic)
    val parentChildrenTable = builder.table[String, ChildIdSet](config.parentChildrenTopic)
    val destTable = builder.table[String, Lineage](config.destTopic)

    // Index 0 will contain all children which set themselves as their own parent
    // Index 1 will contain all otherwise valid child/parent relationships
    val splitSources = sourceTable.toStream.branch(
      (childId: String, parentId: String) => childId == parentId,
      (_, _) => true
    )

    // Children who are their own parents are immediately undefined
    splitSources(0)
      .map((childId: String, _) => {
        _log.warn("Rejected ID with itself as parent: {}", childId)
        (childId, Lineage(Reserved.UNDEFINED))
      })
      .to(config.destTopic)

    splitSources(1)
      .leftJoin(parentTransitionTable, (newParentId: String, priorTransition: ParentTransition) =>
          ParentTransition(Option(priorTransition).map(t => t.newParentId).getOrElse(Reserved.UNDEFINED), newParentId))
      .filter((_, parentTransition: ParentTransition) => !parentTransition.isRedundant)
      .to(config.parentTransitionTopic)

    parentTransitionTable
      .toStream
      .flatMap((childId: String, parentTransition: ParentTransition) => {
        Seq(
          (parentTransition.oldParentId, ChildTransition.remove(childId)),
          (parentTransition.newParentId, ChildTransition.add(childId)))
      })
      .to(config.childTransitionTopic)

    // Root parents
    childTransitionStream
      .filter((id: String, transition: ChildTransition) => id == Reserved.ROOT && transition.isAdd)
      .map((_, transition: ChildTransition) => (transition.childId, Lineage.empty))
      .to(config.destTopic)

    // Parents which don't yet exist
    childTransitionStream
      .leftJoin(sourceTable, (_: ChildTransition, maybeParent: String) => Option(maybeParent).isDefined)
      .filter((id: String, exists: Boolean) => !exists && !(Reserved isReserved id))
      .map((id: String, _) => (id, Lineage(Reserved.UNDEFINED)))
      .to(config.destTopic)

    childTransitionStream
      .groupByKey
      .aggregate(
        () => ChildIdSet.empty,
        (_: String, transition: ChildTransition, childIds: ChildIdSet) => childIds + transition,
        materializedParentChildren)
      .toStream
      .to(config.parentChildrenTopic)

    parentChildrenTable
      .join(destTable, (children: ChildIdSet, lineage: Lineage) => LineageTransition(lineage, children))
      .toStream
      .flatMap((id: String, transition: LineageTransition) => {
        val unsafeUpdatedLineage = transition.parentLineage :+ id
        unsafeUpdatedLineage match {
            case None =>
              _log.warn("Rejected update which creates a lineage cycle for {}", id)
              Seq.empty
            case Some(updatedLineage : Lineage) =>
              transition.children.map((childId : String) => (childId, updatedLineage))
          }
        })
      // Filter out updates which don't change the child's lineage at the destination
      .leftJoin(destTable, (newLineage: Lineage, oldLineage: Lineage) => if (oldLineage != newLineage) Option(newLineage) else Option.empty)
      .filter((_: String, lineage: Option[Lineage]) => lineage.isDefined)
      .map((id: String, lineage: Option[Lineage]) => (id, lineage.get))
      .to(config.destTopic)

    builder.build
  }
}
