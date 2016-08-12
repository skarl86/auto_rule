package spark.util

import java.text.SimpleDateFormat
import java.util.{NoSuchElementException, Calendar}

import org.apache.log4j.{Logger, Level}

import scala.util.matching.Regex

/**
  * Created by NK on 2016. 6. 21..
  */
object Util {
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

    "output/" + List(path, currentMinuteAsString).mkString("-")
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
      try{
        val tokens = TripleParser.findAllIn(line)
        val (s, p, o) = (tokens.next(), tokens.next(), tokens.next())
        (s, p, o)

      }catch {
        case nse: NoSuchElementException => {
          ("ERROR", "ERROR", "ERROR")
        }
      }
    }

  }

}
