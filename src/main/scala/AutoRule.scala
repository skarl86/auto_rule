import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.util.matching.Regex

/**
  * Created by NK on 2016. 6. 9..
  */
object AutoRule {
  type Triple = (String, String, String)
  type Tuple = (String, String)

  val RDF_LABEL = "<http://www.w3.org/2000/01/rdf-schema#label>"
  val RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
  val DATAFRAME_OUTPUT_FORMAT = "com.databricks.spark.csv"

  def main(args: Array[String]) {
    setLogLevel(Level.WARN)

    val conf = new SparkConf().setAppName("Auto Rule").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val path = "data/PM_fi.0.n3"

    val inputTripleRDD = sc.textFile(path).mapPartitions(NTripleParser, true)
    val hasVisualAndHasAuralTripleRDD = getHasVisualAndHasAural(inputTripleRDD)
    val objectLabelTupleRDD = getLabelTriple(inputTripleRDD)
    val objectTypeTupleRDD = getTypeTriple(inputTripleRDD)

    // 전체 Triple 중에 Golf Event만 추출한 RDD
    val golfEventTripleRDD = getGolfEventTripleRDD(hasVisualAndHasAuralTripleRDD)

    // Golｆ Event의 Shot에 포함 된 Object 별 갯수 정보를 가지고 있는 RDD
    val totalCountRDD = getNumberOfObjectInShotRDD(golfEventTripleRDD, true)
    totalCountRDD.coalesce(1).saveAsTextFile(generatePath("total_object_in_shot"))

    // Golf Event의 Shot마다 Object의 갯수 정보를 DataFrame으로 만들기 위한
    // 사전 작업으로 DataFrame의 Header 에 해당하는 Field Name을 가져오기 위한 작업.
    val objectNameFieldNames = golfEventTripleRDD.map{case (s, p, o) => eraseIndex(eraseVideoIDString(o))}.distinct().collect().toList

    // DataFrame의 기반이 될 RDD로
    // Event의 Shot 마다 각각의 Object 갯수 정보를 가지고 있는 RDD.
    val resultRDD = getCountEachShotObjectNameInVideoRDD(golfEventTripleRDD)

    // Event의 Shot 마다 각각의 Object 갯수 정보를 가지고 있는 RDD를
    // 기반으로 DataFrame 생성.
    val resultDF = getCountEachShotInVideoDF(resultRDD, objectNameFieldNames, sqlContext)
    resultDF.coalesce(1).write
      .format(DATAFRAME_OUTPUT_FORMAT).option("header", "true")
      .save(generatePath("count_each_shot_object_name_in_video"))
  }

  /**
    * * RDD 를 파일로 저장할 때
    * 저장 경로를 중복되지 않도록
    * Unique한 주소를 생성 해주는 함수
 *
    * @param path 사용자가 직접 입력한 경로.
    * @return Unique 한 path
    */
  def generatePath(path:String = ""): String ={
    val now = Calendar.getInstance().getTime()
    val minuteFormat = new SimpleDateFormat("yyyyhhmmss")
    val currentMinuteAsString = minuteFormat.format(now)

    List(path, currentMinuteAsString).mkString("-")
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

  /**
    * Row의 값을 넣어주기 위한 사전 처리 작업을
    * 수행하는 함수.
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
    * @param level
    */
  def setLogLevel(level: Level): Unit = {
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
  }

  /**
    *
    * @param lines
    * @return
    */
  def NTripleParser(lines: Iterator[String]) = {
    val TripleParser = new Regex("(<[^\\s]*>)|(_:[^\\s]*)|(\".*\")")
    for (line <- lines) yield {
      val tokens = TripleParser.findAllIn(line)
      val (s, p, o) = (tokens.next(), tokens.next(), tokens.next())
      (s, p, o)
    }
  }

  /**
    *
    * @param triple
    * @return
    */
  def getTypeTriple(triple: RDD[Triple]): RDD[Tuple] = { getPredicate(triple, RDF_TYPE) }

  /**
    *
    * @param triple
    * @return
    */
  def getLabelTriple(triple: RDD[Triple]): RDD[Tuple] ={ getPredicate(triple, RDF_LABEL) }

  /**
    *
    * @param triple
    * @param predicate
    * @return
    */
  def getPredicate(triple: RDD[Triple], predicate:String): RDD[Tuple] = {
    triple
      .filter{case (s, p, o) => p.contains(predicate)}
      .map{case (s, p, o) => (s, o)}
  }

  /**
    *
    * @param videoID
    * @return
    */
  def isGolfVideo(videoID: String): Boolean = {
    // Golf Activity는 Video 1번 부터 100번 까지 이다.
    val reg = new Regex("([0-9]+)")
    val tokens = reg.findAllIn(videoID)
    val id = Integer.valueOf(tokens.next())

    return 100 >= id
  }

  /**
    *
    * @param videoID
    * @return
    */
  def eraseVideoIDString(videoID:String): String = {
    val reg = new Regex("_(.+)>")
    reg.findAllIn(videoID).matchData.next().group(1)
  }

  /**
    *
    * @param objectName
    * @return
    */
  def eraseIndex(objectName:String): String = {
    val reg = new Regex("(\\w+[a-z]+)")
    reg.findAllIn(objectName).matchData.next().group(1)
  }

  /**
    *
    * @param str
    * @return
    */
  def eraseURI(str:String): String ={
    val reg = new Regex("([A-Z]\\w+)")
    if(reg == null)
    {
      str
    }else{
      reg.findAllIn(str).matchData.next().group(1)
    }
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
    * @param tripleRDD
    * @return
    */
  def getGolfEventTripleRDD(tripleRDD: RDD[Triple]): RDD[Triple] = {
    tripleRDD.filter{case (s, p, o) => isGolfVideo(s)}
  }

  /**
    *
    * @param tripleRDD
    * @return
    */
  def makeSchema(tripleRDD: RDD[Triple]): RDD[String] = {
    tripleRDD.filter{case (s, p, o) => p.contains("#type")}.map{case(s, p, o) => o}.distinct()
  }
}
