import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

import ontology.MediaOntology._

import spark.util.Util._

/**
  * Created by NK on 2016. 6. 9..
  */
object AutoRule {

  val DATAFRAME_OUTPUT_FORMAT = "com.databricks.spark.csv"

  def main(args: Array[String]) {
    setLogLevel(Level.WARN)

    val conf = new SparkConf().setAppName("Auto Rule").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val path = "data/PM_fi.0.n3"

    val inputTripleRDD = sc.textFile(path).mapPartitions(NTripleParser, true)
    val eventRangeMap = Map(
      "Golf" -> Range(1, 100),
      "Hiking" -> Range(101, 128),
      "Graduate" -> Range(129, 226),
      "Bicycle" -> Range(270, 369),
      "Birthday" -> Range(370, 454),
      "Toddle" -> Range(455, 554)
    )

    eventRangeMap.foreach{ case (key, value) =>
      val eventRDD = getEventTripleRDD(inputTripleRDD, value)
      println("=================================================")
      println(key + " Event")
      println("=================================================")
      val resultRDD = getAutoDescription(eventRDD, sqlContext)

      for( (axiom, descs) <- resultRDD.collect() ){
        println(String.format("\nAxiom : [ %s ]\nCandidate Object : [ %s ]", axiom, descs.mkString(", ")))
      }
      println("=================================================")
    }
  }
}
