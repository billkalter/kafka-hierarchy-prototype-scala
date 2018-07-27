package com.bazaarvoice.legion.hierarchy.model

import scala.collection.immutable.Seq

object Lineage {

  val empty = new Lineage(Seq.empty[String])
  
  def apply(parents : Seq[String]) : Lineage = {
    new Lineage(parents)
  }

  def apply(onlyParent : String) : Lineage = {
    new Lineage(Seq(onlyParent))
  }
}

case class Lineage(parents : Seq[String]) {

  def :+(tailChildId : String) : Option[Lineage] = {
    if (parents contains tailChildId) {
      None
    } else {
      Some(Lineage(parents :+ tailChildId))
    }
  }
}