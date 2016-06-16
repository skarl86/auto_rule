import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.matching.Regex

/**
  * Created by NK on 2016. 6. 9..
  */
object AutoRule {
  type Triple = (String, String, String)
  type Tuple = (String, String)

  val RDF_LABEL = "<http://www.w3.org/2000/01/rdf-schema#label>"
  val RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

  def main(args: Array[String]) {
    setLogLevel(Level.WARN)

    val conf = new SparkConf().setAppName("Auto Rule").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val path = "data/PM_fi.0.n3"

    val tripleRDD = sc.textFile(path).mapPartitions(NTripleParser, true)
    val hasVisualAndHasAuralTripleRDD = getHasVisualAndHasAural(tripleRDD)
    val objectLabelTupleRDD = getLabel(tripleRDD)
    val objectTypeTupleRDD = getType(tripleRDD)
    val golfActivityTripleRDD = getGolfActivityRDD(hasVisualAndHasAuralTripleRDD)

    getObjectCountInShot(golfActivityTripleRDD, objectLabelTupleRDD)
//    getTotalShotCount(golfActivityTripleRDD)
//    getTotalShotCount(golfActivityTripleRDD, true)
//    getTotalLabelCount(objectLabelTupleRDD, golfActivityTripleRDD)
//    golfActivityRDD
//      .map{ case (s, p, o) => (eraseVideoIDString(o) , 1)}
//      .map{ case (k, v) => (eraseIndex(k), v) }
//      .reduceByKey(_ + _)
//      .sortBy(_._2, false)
//      .coalesce(1).saveAsTextFile("output")
  }

  def getObjectCountInShot(golfActivityTripleRDD:RDD[Triple], typeTripleRDD:RDD[Tuple]) = {
    val t1 = golfActivityTripleRDD
      .map{case (s, p, o) => (s, o)}
    t1
      .map{case (s, o) => (o, s)}
      .join(typeTripleRDD)
      .map{case (o, (s, c)) =>  ((s, c), 1)}
      .reduceByKey(_ + _)
      .map{case ((s, c), count) => (eraseURI(s), (c, count))}
      .groupByKey()
      .map{case (s, obj_buffer) => (s, obj_buffer.toList)}
      .sortBy(_._1, true)
      .coalesce(1).saveAsTextFile("output2")
  }

  def getTotalLabelCount(objectLabelTripleRDD:RDD[Tuple], activityTripleRDD:RDD[Triple]): Unit = {
    val objectCountRDD = activityTripleRDD
      .map{ case (s, p, o) => (o, 1) }
      .reduceByKey(_ + _)
    objectLabelTripleRDD
      .join(objectCountRDD)
      .map{ case (o, (l, c)) => (l, c)}
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .coalesce(1).saveAsTextFile("labelCount")
  }

  def getTotalShotCount(activityTripleRDD:RDD[Triple], withoutIndex:Boolean = false) ={
    var rstRDD:RDD[(String, Int)] = activityTripleRDD.map{ case (s, p, o) => (eraseVideoIDString(o) , 1)}
    val path = if(withoutIndex) { "objectCountWithoutIndex" } else { "objectCount" }
    if(withoutIndex){
      rstRDD = rstRDD
        .map{ case (k, v) => (eraseIndex(k), v)}
    }
    rstRDD
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .coalesce(1).saveAsTextFile(path)

  }
  def setLogLevel(level: Level): Unit = {
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
  }

  def NTripleParser(lines: Iterator[String]) = {
    val TripleParser = new Regex("(<[^\\s]*>)|(_:[^\\s]*)|(\".*\")")
    for (line <- lines) yield {
      val tokens = TripleParser.findAllIn(line)
      val (s, p, o) = (tokens.next(), tokens.next(), tokens.next())
      //      if (p == RDF_TYPE) (s, "type", o)
      //      else (s, p, o)
      (s, p, o)
    }
  }

  def getType(triple: RDD[Triple]): RDD[Tuple] = { getPredicate(triple, RDF_TYPE) }
  def getLabel(triple: RDD[Triple]): RDD[Tuple] ={ getPredicate(triple, RDF_LABEL) }

  def getPredicate(triple: RDD[Triple], predicate:String): RDD[Tuple] = {
    triple
      .filter{case (s, p, o) => p.contains(predicate)}
      .map{case (s, p, o) => (s, o)}
  }
  def isGolfVideo(videoID: String): Boolean = {
    // Golf Activity는 Video 1번 부터 100번 까지 이다.
    val reg = new Regex("([0-9]+)")
    val tokens = reg.findAllIn(videoID)
    val id = Integer.valueOf(tokens.next())

    return 100 >= id
  }

  def eraseVideoIDString(videoID:String): String = {
    val reg = new Regex("_(.+)>")
    reg.findAllIn(videoID).matchData.next().group(1)
  }
  def eraseIndex(objectName:String): String = {
    val reg = new Regex("(\\w+[a-z]+)")
    reg.findAllIn(objectName).matchData.next().group(1)
  }
  def eraseURI(str:String): String ={
    val reg = new Regex("([A-Z]\\w+)")
    if(reg == null)
    {
      return str
    }else{
      reg.findAllIn(str).matchData.next().group(1)
    }

  }

  def getHasVisualAndHasAural(tripleRDD: RDD[Triple]) = {
    val hasVisualTripleRDD = tripleRDD.filter{case (s, p, o) => p.contains("hasVisual")}
    val hasAuralTripleRDD = tripleRDD.filter{case (s, p, o) => p.contains("hasAural")}

    hasAuralTripleRDD.union(hasVisualTripleRDD)
  }
  def getGolfActivityRDD(tripleRDD: RDD[Triple]): RDD[Triple] = {
    tripleRDD.filter{case (s, p, o) => isGolfVideo(s)}
  }

  def makeSchema(tripleRDD: RDD[Triple]): RDD[String] = {
    tripleRDD.filter{case (s, p, o) => p.contains("#type")}.map{case(s, p, o) => o}.distinct()
  }
}
