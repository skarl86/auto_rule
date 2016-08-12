package ontology.util

import ontology.MediaOntology._
/**
  * Created by NK on 2016. 8. 11..
  */
object FilterOption {
  def isObject(str:String) = {
    str.contains(HAS_AURAL) || str.contains(HAS_VISUAL) || str.contains(WHAT_OBJECT)
  }
}
