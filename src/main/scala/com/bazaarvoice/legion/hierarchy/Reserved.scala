package com.bazaarvoice.legion.hierarchy

object Reserved {

  val ROOT = "__root__"
  val UNDEFINED = "__undefined__"

  def isReserved(id : String) : Boolean = {
    id == ROOT || id == UNDEFINED
  }
}
