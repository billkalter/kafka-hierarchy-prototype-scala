package com.bazaarvoice.legion.hierarchy.model

import scala.collection.immutable.Set

object ChildIdSet {
  def apply(children : Set[String]) : ChildIdSet = {
    new ChildIdSet(children)
  }

  def apply(onlyChild : String) : ChildIdSet = {
    new ChildIdSet(Set(onlyChild))
  }
  
  val empty = ChildIdSet(Set.empty[String])
}

case class ChildIdSet(children : Set[String]) {

  def +(transition : ChildTransition) : ChildIdSet = {
    if (transition.isAdd) {
      if (!(children contains transition.childId)) {
        return ChildIdSet(children + transition.childId)
      }
    } else if (children contains transition.childId) {
      return ChildIdSet(children - transition.childId)
    }
    this
  }

  def map[T](f: String => T) : Seq[T] = {
    children.toList.map(f)
  }
}
