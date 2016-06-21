package spark

import ontology.util.StringUtil._
import ontology.MediaOntology._
import org.apache.spark.rdd.RDD
import spark.util.Util._

/**
  * Created by NK on 2016. 6. 21..
  */
object Generator {
  def generateClassAssertion(golfEventTripleRDD:RDD[Triple]) = {
    golfEventTripleRDD
      .map{case (s, p, o) => (eraseIndex(eraseVideoIDString(o)), eraseVideoIDString(o))}.distinct()
      .map{case (withoutIndexO, o) =>
        val splits = withoutIndexO.split("_")
        var className = ""
        if(splits.size > 1){
          for(str <- splits){
            className = className.concat(str.capitalize)
          }
          (o, className)
        }else{
          (o, withoutIndexO.capitalize)
        }
      }
      .map{case (objectName, className) =>
        String.format(
          "<ClassAssertion>\n" +
            "        <Class IRI=\"#%s\"/>\n" +
            "        <NamedIndividual IRI=\"#%s\"/>\n    " +
            "</ClassAssertion>"
          ,className, objectName)
      }.coalesce(1).saveAsTextFile(generatePath("class"))
  }

  def generateObjectPropertyAssertion(golfEventTripleRDD:RDD[Triple]) = {
    golfEventTripleRDD
      .map{case (s, p, o) => String.format(
        "<ObjectPropertyAssertion>\n        " +
          "<ObjectProperty IRI=\"#%s\"/>\n        " +
          "<NamedIndividual IRI=\"#%s\"/>\n        " +
          "<NamedIndividual IRI=\"#%s\"/>\n    " +
          "</ObjectPropertyAssertion>"
        ,getProperty(p), eraseURI(s).toLowerCase, eraseVideoIDString(o))}
      .coalesce(1).saveAsTextFile(generatePath("object-property"))
  }
  def generateDeclaration(golfEventTripleRDD:RDD[Triple]) = {
    golfEventTripleRDD
      .map{case (s, p, o) => String.format(
        "<Declaration>\n        " +
          "<NamedIndividual IRI=\"#%s\"/>\n    " +
          "</Declaration>"
        ,eraseURI(s).toLowerCase)}
      .coalesce(1).saveAsTextFile(generatePath("decl"))
  }
}
