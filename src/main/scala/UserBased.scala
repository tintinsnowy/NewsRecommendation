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
 event.csv: event_id, user_id, start_time, city, state, zip, country, lat, and lng
 event_attendees.csv: event_id, yes, maybe, invited, and no.
 https://wizardforcel.gitbooks.io/w3school-scala/content/17.html
*/
object UserBased{
	 // def main(args:Array[String]) {

  //   //avoid displaying the log on the screen    
        
  //     var userData = new Array[(Int,String,String,String,String)](0)
  //     var userMax:Map[String,Int] = Map()

  //     for (s <- SingleUser){
  //         if(!userMax.contains(s._1))
  //            userMax += (s._1 -> s._2(1)._1)
  //         val temp_array = concat(s._2.toArray,userData)
  //         userData = temp_array
  //     }// the map from username to maxindex
  //     val similarity_Max = UserSimilarity(userData, userMax.size)
  // }//end of main
  //match and calculate the similarity
  def UserRecommend(similarity_Max: Array[Array[Double]],top: Int
        ,userMax:Array[(String,List[(Int,Long)])]):
        Array[List[Long]] = {
      // then we start recommendation 
      val len_sim = similarity_Max(1).size.toInt-1
      
      val recomUser_Max = new Array[List[Long]](len_sim)
       
       for (i  <- 0 to len_sim-1){
    
        val temp_s = similarity_Max(i).sortWith(_.compareTo(_) > 0)
    
        var union_recomm = Array[Long](0)  
        
        for (j <- 0 to (top-1)){
          
            userMax2(j)._2.foreach(
               s =>
               if(!union_recomm.exists({x: Long => x==s._2}))
                 union_recomm = union_recomm :+ s._2
              )
        }

        userMax2(i)._2.foreach{
            s => 
            if(union_recomm.exists({x: Long => x==s._2}))
            
               union_recomm = union_recomm.filter(x => x!=s._2)}
               
        recomUser_Max(i) = union_recomm.toList
    }
    return recomUser_Max
 }//end of function

def UserSimilarity(userData: Array[(Int,String,String,String,String)], len: Int):
      
      Array[Array[Double]] = {
            
      var result = ofDim[Double](len,len)
      println("====hello====")
      for (i <- 0 to len){
    
       for (j <- 0 to len){
          
          val len_a = userData(i)._1
    
          val len_b = userData(j)._1
      
          if (i != j && ((userData(i)._2) == userData(j)._2)){
    
            var common  = 0 
  
            if (userData(i)._3 == userData(j)._3)
              common = common + 1
            if (userData(i)._5 == userData(j)._5)
              common = common + 1
            if (userData(i)._4 == userData(j)._4)
              common = common + 1
            result(len_a)(len_b) = (result(len_a)(len_b) + common/3.0)
          }
       }
    }
     return result
} // end of the UserSimilarity



  // 用户-用户共现矩阵
  /*
  This function is used to format the userData. 
  */
  def UserAction(parsedData: Array[(String, String, String, String, String)] ): 
  
   Map[String,List[(Int,String,String,String,String)]] = {
    
    var temp:Map[String,List[(Int,String,String,String,String)]]= Map()
    
    val total = parsedData.length
    
    var num = 0
    
    for(i <- 0 to (total-1))
    {       
       
       if( temp.contains(parsedData(i)._1) ){ //if already contains 
       
          val temp_num = temp(parsedData(i)._1).last._1
          // This is this data structure of list 
          var listAction = List((temp_num, parsedData(i)._2, parsedData(i)._3, parsedData(i)._4, parsedData(i)._5))

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
// ==
}
