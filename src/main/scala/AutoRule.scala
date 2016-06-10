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

  def isGolfVideo(videoID: String): Boolean = {
    // Golf Activity는 Video 1번 부터 100번 까지 이다.
    val reg = new Regex("([0-9]+)")
    val tokens = reg.findAllIn(videoID)
    val id = Integer.valueOf(tokens.next())

    return 100 >= id
  }

  def eraseVideoIDString(videoID:String): Any = {
    val reg = new Regex("_(.+)")
    reg.findAllIn(videoID).matchData.next().group(1)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Auto Rule").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val path = "data/PM_fi.0.n3"

    val tripleRDD = sc.textFile(path).mapPartitions(NTripleParser, true)

    val hasVisualAndHasAuralTripleRDD = getHasVisualAndHasAural(tripleRDD)

    val golfActivityRDD = getGolfActivityRDD(hasVisualAndHasAuralTripleRDD)
//    golfActivityRDD.map{ case (s, p, o) => (eraseVideoIDString(o) , 1)}.reduceByKey(_ + _).sortBy(_._2, false).coalesce(1).saveAsTextFile("output")
//    print(golfActivityRDD.count())
  }

  def getHasVisualAndHasAural(tripleRDD: RDD[Triple]) = {
    val hasVisualTripleRDD = tripleRDD.filter{case (s, p, o) => p.contains("hasVisual")}
    val hasAuralTripleRDD = tripleRDD.filter{case (s, p, o) => p.contains("hasAural")}

    hasAuralTripleRDD.union(hasVisualTripleRDD)
  }
  def getGolfActivityRDD(tripleRDD: RDD[Triple]): RDD[Triple] = {
    tripleRDD.filter{case (s, p, o) => isGolfVideo(s)}
  }
}
