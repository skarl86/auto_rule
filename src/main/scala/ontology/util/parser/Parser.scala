package ontology.util.parser

import ontology.MediaOntology._
import org.apache.spark.rdd.RDD

/**
  * Created by NK on 2016. 6. 21..
  */
object Parser {
  val RDF_LABEL = "<http://www.w3.org/2000/01/rdf-schema#label>"
  val RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

  /**
    *
    * @param triple
    * @return
    */
  def getTypeTriple(triple: RDD[Triple]): RDD[Triple] = { getPredicate(triple, RDF_TYPE) }

  /**
    *
    * @param triple
    * @return
    */
  def getLabelTriple(triple: RDD[Triple]): RDD[Triple] ={ getPredicate(triple, RDF_LABEL) }

  /**
    *
    * @param triple
    * @param predicate
    * @return
    */
  def getPredicate(triple: RDD[Triple], predicate:String): RDD[Triple] = {
    triple
      .filter{case (s, p, o) => p.contains(predicate)}
  }
}
