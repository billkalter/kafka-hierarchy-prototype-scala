package com.bazaarvoice.legion.hierarchy.model

object ChildTransition {
  def add(childId : String) : ChildTransition = {
    new ChildTransition(childId, true)
  }

  def remove(childId : String) : ChildTransition = {
    new ChildTransition(childId, false)
  }
}
case class ChildTransition(childId : String, isAdd : Boolean)
