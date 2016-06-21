package ontology

import java.util.NoSuchElementException

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.Map
import scala.util.matching.Regex

import ontology.util.StringUtil._

/**
  * Created by NK on 2016. 6. 21..
  */
object MediaOntology {
  type Triple = (String, String, String)
  type Tuple = (String, String)

  val WHAT_BEHAVIOR = "hasWhatBehavior"
  val WHAT_OBJECT = "hasWhatObject"

  def getTotalCount(videoIndexRange:Range, mediaTripleRDD:RDD[Triple]):RDD[(String, Int)] = {
    val eventRDD = getEventTripleRDD(mediaTripleRDD, videoIndexRange)
    getNumberOfObjectInShotRDD(eventRDD, true)
  }

  /**
    *
    * @param tripleRDD
    * @return
    */
  def getGolfEventTripleRDD(tripleRDD: RDD[Triple]): RDD[Triple] = {
    tripleRDD.filter{case (s, p, o) => isGolfVideo(s)}
  }

  /**
    *
    * @param tripleRDD
    * @param videoIDRange
    * @return
    */
  def getEventTripleRDD(tripleRDD: RDD[Triple], videoIDRange: Range) = {
    tripleRDD.filter{case (s, p, o) => isEventVideo(s, videoIDRange)}
  }

  /**
    *
    * @param tripleRDD
    * @return
    */
  def getHasVisualAndHasAural(tripleRDD: RDD[Triple]) = {
    val hasVisualTripleRDD = tripleRDD.filter{case (s, p, o) => p.contains("hasVisual")}
    val hasAuralTripleRDD = tripleRDD.filter{case (s, p, o) => p.contains("hasAural")}

    hasAuralTripleRDD.union(hasVisualTripleRDD)
  }

  /**
    *
    * @param eventTripleRDD
    * @param withoutIndex
    * @return
    */
  def getNumberOfObjectInShotRDD(eventTripleRDD:RDD[Triple], withoutIndex:Boolean = false) ={
    var rstRDD:RDD[(String, Int)] = eventTripleRDD.map{ case (s, p, o) => (eraseVideoIDString(o) , 1)}
    val path = if(withoutIndex) { "objectCountWithoutIndex" } else { "objectCount" }
    if(withoutIndex){
      rstRDD = rstRDD
        .map{ case (k, v) => (eraseIndex(k), v)}
    }
    rstRDD
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

  }

  /**
    *
    * @param tripleRDD
    * @return
    */
  def makeSchema(tripleRDD: RDD[Triple]): RDD[String] = {
    tripleRDD.filter{case (s, p, o) => p.contains("#type")}.map{case(s, p, o) => o}.distinct()
  }

  /**
    *
    * @param objectLabelTripleRDD
    * @param activityTripleRDD
    * @return
    */
  def getTotalLabelCount(objectLabelTripleRDD:RDD[Tuple], activityTripleRDD:RDD[Triple]): RDD[(String, Int)] = {
    val objectCountRDD = activityTripleRDD
      .map{ case (s, p, o) => (o, 1) }
      .reduceByKey(_ + _)
    objectLabelTripleRDD
      .join(objectCountRDD)
      .map{ case (o, (l, c)) => (l, c)}
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
  }

  def getOneHotEncodeDF(eventTriple: RDD[Triple], sqlContext: SQLContext) = {
    val hasVisualAndAuralTripleRDD = getHasVisualAndHasAural(eventTriple)
    val countEachShotInEventRDD = getCountEachShotObjectNameInVideoRDD(hasVisualAndAuralTripleRDD)
    val objectNameFieldNames = hasVisualAndAuralTripleRDD.map{case (s, p, o) => eraseIndex(eraseVideoIDString(o))}.distinct().collect().toList

    val rowRDD = countEachShotInEventRDD
      .map{case (videoShotID, elementList) => convertListToOneHotRow(videoShotID, elementList, objectNameFieldNames)}
    val newFieldNames = List("VideoShotID") ++ objectNameFieldNames
    val schema = StructType(
      newFieldNames.map {
        case fieldName@"VideoShotID" => StructField(fieldName, StringType, true)
        case fieldName => StructField(fieldName, IntegerType, true)
      }
    )
    sqlContext.createDataFrame(rowRDD, schema)
  }

  /**
    * 각 Shot의 포함된 Object에 갯수를
    * DataFrame으로 만들기 전 Row 형태로 변환이 필요한데,
    * 이를 위해 만든 함수
    *
    * @param videoShotID  shot의 고유 ID
    * @param list          ("Golf", golf object의 갯수)
    * @param fieldNames   DataFrame의 StructType을 만들기 위한 테이블의 Field 이름 리스트
    * @return               (ShotID, object의 갯수, object의 갯수, ... )
    */
  def convertListToRow(videoShotID:String, list:List[(String, Int)], fieldNames:List[String]): Row = {
    val states = initMap(fieldNames)
    for((c, count) <- list){
      states(c) = count
    }
    Row.fromSeq(List(videoShotID) ++ fieldNames.map(fieldName => states(fieldName)))
  }

  def convertListToOneHotRow(videoShotID:String, list:List[(String, Int)], fieldNames:List[String]): Row = {
    val states = initMap(fieldNames)
    for((c, count) <- list){
      if(count >= 1){
        states(c) = 1
      }else {
        states(c) = 0
      }
    }
    Row.fromSeq(List(videoShotID) ++ fieldNames.map(fieldName => states(fieldName)))
  }

  /**
    * Row의 값을 넣어주기 위한 사전 처리 작업을
    * 수행하는 함수.
    *
    * @param fieldNames Field 이름 리스트
    * @return             (Golf -> 1), (Person -> 2) ... 와 같은 형태의 Map.
    */
  def initMap(fieldNames:List[String]): scala.collection.mutable.Map[String, Int] = {
    val map = scala.collection.mutable.Map[String, Int]()
    for (fieldName <- fieldNames){
      map(fieldName) = 0
    }
    map
  }

  /**
    * 각 Shot의 포함된 Object에 갯수를
    * DataFrame으로 만들어주는 함수.
    *
    * @param countEachShotInVideoRDD  새로운 DataFrame의 기반이 되는 RDD
    * @param fieldNames                 DataFrame 의 Header에 들어갈 Field 이름.
    * @param sqlContext                 DataFrame을 만들기 위한 SQL Context
    * @return                            DataFrame 객체.
    */
  def getCountEachShotInVideoDF(countEachShotInVideoRDD:RDD[(String, List[(String, Int)])], fieldNames:List[String], sqlContext:SQLContext) = {
    val rowRDD = countEachShotInVideoRDD
      .map{case (videoShotID, elementList) => convertListToRow(videoShotID, elementList, fieldNames)}
    val newFieldNames = List("VideoShotID") ++ fieldNames
    val schema = StructType(
      newFieldNames.map {
        case fieldName@"VideoShotID" => StructField(fieldName, StringType, true)
        case fieldName => StructField(fieldName, IntegerType, true)
      }
    )
    sqlContext.createDataFrame(rowRDD, schema)
  }

  /**
    *
    * @param golfEventTripleRDD
    * @param typeTripleRDD
    * @return
    */
  def getCountEachShotClassInVideoRDD(golfEventTripleRDD:RDD[Triple], typeTripleRDD:RDD[Tuple]) = {
    val countEachShotInVideoRDD = golfEventTripleRDD
      .map{case (s, p, o) => (s, o)}
    countEachShotInVideoRDD
      .map{case (s, o) => (o, s)}
      .join(typeTripleRDD)
      .map{case (o, (s, c)) =>  ((s, c), 1)}
      .reduceByKey(_ + _)
      .map{case ((s, c), count) => (eraseURI(s), (eraseURI(c), count))}
      .groupByKey()
      .map{case (s, obj_buffer) => (s, obj_buffer.toList.sorted)}
      .sortBy(_._1, true)
  }

  /**
    *
    * @param golfEventTripleRDD
    * @return
    */
  def getCountEachShotObjectNameInVideoRDD(golfEventTripleRDD:RDD[Triple]) = {
    val countEachShotInVideoRDD = golfEventTripleRDD
      .map{case (s, p, o) => (eraseURI(s), o)}
    countEachShotInVideoRDD
      .map{case (s, o) =>  ((s, eraseIndex(eraseVideoIDString(o))), 1)}
      .reduceByKey(_ + _)
      .map{case ((s, o_name), count) => (s, (o_name, count))}
      .groupByKey()
      .map{case (s, obj_buffer) => (s, obj_buffer.toList.sorted)}
      .sortBy(_._1, true)
  }

  /**
    *
    * @param eventTripleRDD
    * @param labelTripleRDD
    * @return
    */
  def getTempActivtyType(eventTripleRDD:RDD[Triple], labelTripleRDD:RDD[Triple]) = {
    val labelMap = labelTripleRDD.map{case (s, p, o) => (s, o)}.collectAsMap()
    val activityTupleRDD = eventTripleRDD
      .filter{case (s, p, o) => p.contains(WHAT_BEHAVIOR) || p.contains(WHAT_OBJECT)}
      .map{case (s, p, o) => (s, (p, o))}
      .groupByKey()
      .map{case (s, activities) => (eraseURI(s), changeObjectToName(activities, labelMap))}
      .sortByKey()
    activityTupleRDD

  }


  def changeObjectToName(iterator: Iterable[Tuple], labelMap :Map[String, String]) ={
    var activityList = List[String]()
    for((p, o) <- iterator){
      activityList = activityList ++ List(labelMap(o))
    }
    activityList.mkString("-")
  }

  /**
    *
    * @param videoID
    * @return
    */
  def isGolfVideo(videoID: String): Boolean = {
    // Golf Activity는 Video 1번 부터 100번 까지 이다.
    isEventVideo(videoID, Range(1,100))
  }

  /**
    *
    * @param videoID
    * @param range
    * @return
    */
  def isEventVideo(videoID:String, range: Range): Boolean = {
    val reg = new Regex("([0-9]+)")
    var id = 1000
    try{
      id = reg.findAllIn(videoID).matchData.next().group(1).toInt
      range.start <= id && range.end >= id
    }catch {
      case nse: NoSuchElementException => {
        false
      }
    }
  }
}
