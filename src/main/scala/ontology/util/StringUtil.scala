package ontology.util

import java.util.NoSuchElementException

import scala.util.matching.Regex

/**
  * Created by NK on 2016. 6. 21..
  */
object StringUtil {
  /**
    *
    * @param videoID
    * @param range
    * @return
    */
  def isEventVideo(videoID:String, range: Range): Boolean = {
    val reg = new Regex("([0-9]+)")
    try{
      val id = reg.findAllIn(videoID).matchData.next().group(1).toInt
      range.start <= id && range.end >= id
    }catch {
      case nse: NoSuchElementException => {
        false
      }
    }
  }

  /**
    *
    * @param videoID
    * @return
    */
  def eraseVideoIDString(videoID:String): String = {
    try{
      val reg = new Regex("_(.+)>")
      reg.findAllIn(videoID).matchData.next().group(1)
    } catch {
      case nse: NoSuchElementException => {
        videoID
      }
    }
  }

  /**
    *
    * @param objectName
    * @return
    */
  def eraseIndex(objectName:String): String = {
    try{
      val reg = new Regex("(\\w+[a-z]+)")
      reg.findAllIn(objectName).matchData.next().group(1)
    }catch {
      case nse: NoSuchElementException => {
        objectName
      }
    }

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
    * @param propertyURI
    * @return
    */
  def getProperty(propertyURI: String)= {
    val reg = new Regex("ontology\\/([a-zA-Z]+)")
    reg.findAllIn(propertyURI).matchData.next().group(1)
  }
}
