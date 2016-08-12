import nkutil.encode.Unicode
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

import ontology.MediaOntology._
import spark.util.Util._
import Research._
/**
  * Created by NK on 2016. 7. 26..
  */
object MainProgram {

  val DATAFRAME_OUTPUT_FORMAT = "com.databricks.spark.csv"

  val eventRangeMap = Map(
    "Golf" -> Range(1, 100),
    "Hiking" -> Range(101, 128),
    "Graduate" -> Range(129, 226),
    "Bicycle" -> Range(270, 369),
    "Birthday" -> Range(370, 454),
    "Toddle" -> Range(455, 554)
  )

  def main(args: Array[String]) {
    setLogLevel(Level.WARN)

    val conf = new SparkConf().setAppName("Auto Rule").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val path = "data/PM_fi.0.n3"
    val path2 = "data/personalMedia.n3"

    val inputTripleRDD = sc.textFile(path2).mapPartitions(NTripleParser,true)

    // Shot 총 갯수
    println(Research.getTotalShot(inputTripleRDD))
//    Research.getActivityTripleRDDPerShot(inputTripleRDD)//.map{case (activity, shots) => (activity, shots.length) }.sortBy(_._2,false).coalesce(1).saveAsTextFile(generatePath("Count Activity Per Shot"))
//    Research.getActivityAndObjectsRDD(inputTripleRDD).coalesce(1).saveAsTextFile(generatePath("Activity's Object Count"))
  }

  def getActivityInEvents(path:String, sc:SparkContext) = {
    val inputTripleRDD = sc.textFile(path).mapPartitions(NTripleParser,true)
    Research.getActivityInEvent(inputTripleRDD)
  }
  def getNumberOfActivityInEvent(path:String, sc:SparkContext) = {
    val inputTripleRDD = sc.textFile(path).mapPartitions(NTripleParser,true)
    Research.getNumberOfActivityInEvent(inputTripleRDD)
  }
  def getNumberOfEvents(path:String, sc:SparkContext) = {
    val inputTripleRDD = sc.textFile(path).mapPartitions(NTripleParser,true)
    getEventsTripleRDD(inputTripleRDD).map{case (s, p, o) => s}.collect().foreach(println)
  }
  def getNumberOfActivityInAll(path:String, sc:SparkContext) = {
    val inputTripleRDD = sc.textFile(path).mapPartitions(NTripleParser,true)
    println(Research.getNumberOfActivityInAll(inputTripleRDD).count())

  }
  def getActivity(path:String, sc: SparkContext) = {
    val inputTripleRDD = sc.textFile(path).mapPartitions(NTripleParser,true)
    getHasActivityTripleRDD(inputTripleRDD).coalesce(1).saveAsTextFile(generatePath())
  }

  def foo(sc:SparkContext, sqlContext:SQLContext, path:String) = {
    val inputTripleRDD = sc.textFile(path).mapPartitions(NTripleParser, true)

    eventRangeMap.foreach { case (key, value) =>
        val eventRDD = getEventTripleRDD(inputTripleRDD, value)
        println("=================================================")
        println(key + " Event")
        println("=================================================")
        getActivityAutoDescription(eventRDD, sqlContext)
    }
  }

  def createAutoDescription(sc:SparkContext, sqlContext: SQLContext, path:String) = {
    val inputTripleRDD = sc.textFile(path).mapPartitions(NTripleParser, true)

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
