package com.bazaarvoice.legion.hierarchy.model

import com.bazaarvoice.legion.hierarchy.Reserved

object ParentTransition {
  def apply(oldParentId : String, newParentId : String) : ParentTransition = {
    new ParentTransition(
      Option(oldParentId) getOrElse Reserved.UNDEFINED,
      Option(newParentId) getOrElse Reserved.ROOT)
  }
}

case class ParentTransition(oldParentId : String, newParentId : String) {
  def isRedundant : Boolean = {
    oldParentId == newParentId
  }
}

