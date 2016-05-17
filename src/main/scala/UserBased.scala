import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

import org.apache.spark.rdd._

import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.SparkContext._

import scala._
import java.io.File
import scala.collection.mutable.Map
import Array._
/*    the list of table
 events.csv, and event_attendees.csv
 train.csv: user, event, invited, timestamp, interested, and not_interested
 test.csv: user, event, invited, timestamp
 users.csv: user_id, 
            locale(is a string representing the user's locale, which should be of the form language_territory. ),
            birthyear, gender, joinedAt, location, timezone.
 user_friends.csv: user, friends
 user.csv: user_id, locale, birthyear, gender, joinedAt, location,timezone.
 event.csv: event_id, user_id, start_time, city, state, zip, country, lat, and lng
 event_attendees.csv: event_id, yes, maybe, invited, and no.
 https://wizardforcel.gitbooks.io/w3school-scala/content/17.html
*/
object UserBased{

	 def main(args:Array[String]) {

    //avoid displaying the log on the screen    

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)


    //set the operation environment

    //val out = new java.io.PrintWriter("/home/sherry/web-data/user/"+ s._1)

      val sparkConf = new SparkConf()//.setAppName("UserBased")//.setMaster("local[5]")  //there's no need when you use single node
      val sc = new SparkContext(sparkConf)
      val str = "/home/sherry/web-data/train.csv"
      //val str = "/root/sherry/data/train.csv"
      val data = sc.textFile(str)//args(0)
      var parsedData = data.map(s => (s.split(',')(0).toString, s.split(',')(1).toString,
          s.split(',')(2).toString, s.split(',')(4).toString, s.split(',')(5).toString)).collect
        
      val SingleUser = UserAction(parsedData)
      var userData = new Array[(Int,String,String,String,String)](0)
      var userMax:Map[String,Int] = Map()

      for (s <- SingleUser){
          if(!userMax.contains(s._1))
             userMax += (s._1 -> s._2(1)._1)
          val temp_array = concat(s._2.toArray,userData)
          userData = temp_array
      }// the map from username to maxindex
      val similarity_Max = UserSimilarity(userData, userMax.size)
  }//end of main

  //match and calculate the similarity
  def UserSimilarity(userData: Array[(Int,String,String,String,String)], len: Int):
      Array[Array[Double]] = {
      var result = ofDim[Double](len,len)
      for (i <- 0 to (len-1)){
        for (j <- 0 to (len-1))
            result(i)(j) = 0
      }// initialization
    val len_user = userData.length-1
    for (i <- 0 to len_user){
       for (j <- 0 to len_user){
           if (i != j && (userData(i)._2) == userData(j)._2){
              val len_a = userData(i)._1
              val len_b = userData(j)._1
              var common  = 0;
            if (userData(i)._3 == userData(j)._3)
                   common = common + 1
            if (userData(i)._4 == userData(j)._4)
                   common = common + 1
            if (userData(i)._5 == userData(j)._5)
                   common = common + 1
            
              result(len_a)(len_b) = result(len_a)(len_b) + common/3.0
           }
       }
    }
     return result
  } // end of the UserSimilarity



  // 用户-用户共现矩阵
  def UserAction(parsedData: Array[(String, String, String, String, String)]): 
   Map[String,List[(Int,String,String,String,String)]] = {
    import scala.collection.mutable.Map //avoid misunderstanding we need include this lib
  
    var temp:Map[String,List[(Int,String,String,String,String)]]= Map()
    val total = parsedData.length
    var num = 0
    for(i <- 0 to (total-1))
    {       
       if( temp.contains(parsedData(i)._1) ){ //if already contains 
          val temp_num = temp(parsedData(i)._1).last._1
          var listAction = List((temp_num,parsedData(i)._2, parsedData(i)._3, parsedData(i)._4, parsedData(i)._5))
          
          val newList = temp(parsedData(i)._1):::listAction
          
          temp.remove(parsedData(i)._1)
          
          temp += (parsedData(i)._1 -> newList)
        }
       else {
          var listAction = List((num,parsedData(i)._2, parsedData(i)._3, parsedData(i)._4, parsedData(i)._5))
          temp += (parsedData(i)._1 -> listAction)
          num = num + 1
        }
    }
    return temp
  }//end of def useraction
  

// ===

//     def UserAction(parsedData: RDD[(String, String, String, String, String)]): 
//      Map[String,List[(String,String,String,String)]] = {
//       import scala.collection.mutable.Map //avoid misunderstanding we need include this lib
    
//       var temp:Map[String,List[(String,String,String,String)]]= Map()
      
//       for(s <- parsedData)
//       {
//          val listAction = List((s._2, s._3, s._4, s._5))
//          if( temp.contains(s._1) ){ //if already contains 
//             val newList = temp(s._1):::listAction
//             temp.remove(s._1)
//             temp += (s._1 -> newList)
//           }
//          else {
//             temp += (s._1 -> listAction)
//           }
//       }
//       return temp
//     }//end of def useraction
  
}//end of o
  
}