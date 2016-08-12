
import nkutil.encode.Unicode
import ontology.util.FilterOption
import org.apache.spark.rdd.RDD

import ontology.MediaOntology._
import ontology.util.StringUtil._

import scala.collection.immutable.ListMap

/**
  * Created by NK on 2016. 8. 9..
  */
object Research {
  def main(args: Array[String]) {
  }
  def getEventsTripleRDD(inputRDD:RDD[Triple]) = {
    inputRDD.filter{case ( s, p, o) => o.contains("<http://www.personalmedia.org/soongsil-diquest#Event>")}
  }

  def getActivityInEvent(inputRDD:RDD[Triple]) = {
    val rdd1 = inputRDD.filter{case (s, p, o) => s.contains("Video") && p.contains("type") && o.contains("Event")}.map{case(s ,p, o) => (eraseURI(s), o)}
    val rdd2 = inputRDD.filter{case (s, p, o) => s.contains("Video") && p.contains("hasActivity") && o.contains("Activity")}.map{case(s ,p, o) => (eraseURI(s).split("_")(0), Unicode.decode(eraseURI(o)))}

    val rdd3 = rdd1.join(rdd2)
    val rdd4 = rdd3.map{case(videoName, event_activity_pair) => event_activity_pair}.groupByKey()
    val rdd5 = rdd4.map{case(eventName, activities) => (eventName, activities.toList.distinct)}
    rdd5
  }

  def getNumberOfActivityInEvent(inputRDD:RDD[Triple]) = {
    val rdd1 = getActivityInEvent(inputRDD)
    val rdd2 = rdd1.map{case(eventName, activities) => (eventName, activities.toList.distinct.length)}
    rdd2
  }

  def getNumberOfActivityInAll(inputRDD:RDD[Triple]) = {
    val rdd1 = inputRDD.filter{case (s, p ,o) => p.contains(HAS_ACTIVITY)}.map{case (s, p, o) => Unicode.decode(eraseURI(o))}.distinct()
    rdd1
  }

  def getActivityTripleRDDPerShot(inputRDD:RDD[Triple]) = {
    val rdd1 = inputRDD.filter{case (s, p, o) => p.contains(HAS_ACTIVITY)}.map{case (s, p, o) => (Unicode.decode(eraseURI(o)), s)}.groupByKey().map{case (activity, shots) => (activity, shots.toList)}
    rdd1
  }

  def getAllObjects(inputRDD:RDD[Triple]) = {
    inputRDD.filter{case (s, p, o) => FilterOption.isObject(p)}.map{case (s, p, o) => eraseIndex(eraseVideoIDString(o))}.distinct()
  }

  def getTotalShot(inputRDD:RDD[Triple]) = {
    inputRDD.filter{case (s, p ,o) => p.contains("#type") && o.contains("<http://www.personalmedia.org/soongsil-diquest#Shot>")}.count()
  }

  def getActivityAndObjectsRDD(inputRDD:RDD[Triple]) = {
    val shotAndActivityPairRDD = inputRDD.filter{case (s, p ,o) => p.contains(HAS_ACTIVITY)}.map{case (s, p, a) => (s, a)}
    val shotAndObjectPairRDD = inputRDD.filter{case (s, p ,o) => FilterOption.isObject(p)}.map{case (s, p ,o) => (s, o)}.groupByKey()

    val rdd1 = shotAndActivityPairRDD
      .join(shotAndObjectPairRDD)
      .map{case (s, (a, o)) => (a, o)}.groupByKey()
      .map{case (a, o) => (Unicode.decode(eraseURI(a)), o.flatten.map(obj => eraseIndex(eraseVideoIDString(obj))))}
      .map{case (a, o) =>
        {
          val map = scala.collection.mutable.Map[String, Int]()
          var count = 0
          for(obj <- o){
            if(map.contains(obj)){map(obj) += 1}
            else{map(obj) = 1}
            count += 1
          }
          (a, (count, ListMap(map.toSeq.sortWith(_._2 > _._2):_*)))
        }
      }

    val rdd2 = getActivityTripleRDDPerShot(inputRDD).map{case (a, s) => (a, s.length)}
    rdd1.join(rdd2).map{case (a, b) => (a, b._2, b._1)}.sortBy(_._2, false)
  }
}
