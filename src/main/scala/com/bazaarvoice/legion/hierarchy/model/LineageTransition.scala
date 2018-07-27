package com.bazaarvoice.legion.hierarchy.model

object LineageTransition {
  def apply(parentLineage : Lineage, children : ChildIdSet) : LineageTransition = {
    new LineageTransition(parentLineage, children)
  }
}

case class LineageTransition(parentLineage : Lineage, children : ChildIdSet)
