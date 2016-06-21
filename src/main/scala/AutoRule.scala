
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

import ontology.MediaOntology._

import spark.util.Util._
import ontology.util.parser.Parser._

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

//    eventRangeMap.foreach{ case (key, value) =>
//      val eventRDD = getEventTripleRDD(inputTripleRDD, value)
//      val oneHotEncodeDF = getOneHotEncodeDF(eventRDD, sqlContext)
//      oneHotEncodeDF.coalesce(1).write
//        .format(DATAFRAME_OUTPUT_FORMAT).option("header", "true")
//        .save(generatePath(key+"_one-hot"))
//    }

    val labelTripleRDD = getLabelTriple(inputTripleRDD)

    eventRangeMap.foreach{ case (key, value) =>
      val eventRDD = getEventTripleRDD(inputTripleRDD, value)
      val activtyRDD = getTempActivtyType(eventRDD, labelTripleRDD)
      activtyRDD
        .map{case(shot, activities) => List(shot, activities).mkString(",")}
        .coalesce(1).saveAsTextFile(generatePath(key+"_activities"))
    }



    // 전체 Triple 중에 Golf Event만 추출한 RDD
//    val golfEventTripleRDD = getGolfEventTripleRDD(hasVisualAndHasAuralTripleRDD)
//    golfEventTripleRDD.map{case (s, p, o) =>}
//
//    // Golｆ Event의 Shot에 포함 된 Object 별 갯수 정보를 가지고 있는 RDD
//    val totalCountRDD = getNumberOfObjectInShotRDD(golfEventTripleRDD, true)
//    totalCountRDD.coalesce(1).saveAsTextFile(generatePath("total_object_in_shot"))
//
//    // Golf Event의 Shot마다 Object의 갯수 정보를 DataFrame으로 만들기 위한
//    // 사전 작업으로 DataFrame의 Header 에 해당하는 Field Name을 가져오기 위한 작업.
//    val objectNameFieldNames = golfEventTripleRDD.map{case (s, p, o) => eraseIndex(eraseVideoIDString(o))}.distinct().collect().toList
//
//    // DataFrame의 기반이 될 RDD로
//    // Event의 Shot 마다 각각의 Object 갯수 정보를 가지고 있는 RDD.
//    val resultRDD = getCountEachShotObjectNameInVideoRDD(golfEventTripleRDD)
//
//    // Event의 Shot 마다 각각의 Object 갯수 정보를 가지고 있는 RDD를
//    // 기반으로 DataFrame 생성.
//    val resultDF = getCountEachShotInVideoDF(resultRDD, objectNameFieldNames, sqlContext)
//    resultDF.coalesce(1).write
//      .format(DATAFRAME_OUTPUT_FORMAT).option("header", "true")
//      .save(generatePath("count_each_shot_object_name_in_video"))
//
//    // Event의 Shot 마다 각각의 Object 갯수 정보를 가지고 있는 RDD를
//    // 기반으로 Object가 하나라도 있으면 1, 없으면 0인 DataFrame
//    val oneHotEncodeDF = getOneHotEncodeDF(resultRDD, objectNameFieldNames, sqlContext)
//    oneHotEncodeDF.coalesce(1).write
//      .format(DATAFRAME_OUTPUT_FORMAT).option("header", "true")
//      .save(generatePath("one_hot"))

//    val result = getTempActivtyType(getGolfEventTripleRDD(inputTripleRDD),objectLabelTupleRDD)
//    val eventTripleRDD = getGolfEventTripleRDD(inputTripleRDD).map{case (s, p, o) => List(s, p, o, ".").mkString(" ")}.coalesce(1).saveAsTextFile(generatePath())

//    generateClassAssertion(golfEventTripleRDD)
//    generateObjectPropertyAssertion(golfEventTripleRDD)
//    generateDeclaration(golfEventTripleRDD)

  }
}
