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
  def userRecommend(similarity_Max: Array[Array[(Long, Double)]],top: Int
        ,userMax:Array[String,List[(Int,Int)]):
        Array[List[Long]]{
  // then we start recommendation 
      val len_sim = similarity_Max[1].size-1
      
      val recomUser_Max =  Array[List[Long]](len_sim)
    
      for (i  <- 0 to similarity_Max[1].size-1){
    
        val temp_s = s.sorted(_._2 > _._2)
    
        var union_recomm = Array[Long](0)  
        
        for (j <- 0 top-1){
          
          val  userMax(j)._2.foreach(
               s =>
               if(!union_recomm.exist(s._2))
                 union_recomm = union_recomm :+ s._2
               )
        }
        val userA_event = userMax(i)._2.foreach{
            s => 
            
            if(union_recomm.exist(s._2))
            
               union_recomm.filter(x => x!=s._2)}
               
        recomUser_Max(i) = union_recomm.toList
    }
    
    return recomUser_Max
 }//end of function

  def UserSimilarity(userData: Array[(Int,String,String,String,String)]):
      Array[Array[(Long, Double)]] = {
      val len = userData.size-1
      var result = ofDim[(Long, Double)](len,len)
      
      for (i <- 0 to (len)){
      
        for (j <- 0 to (len))
            result(i)(j) = 0
      }// initialization
      val len_user = userData.length-1
    
    for (i <- 0 to len_user){
    
       for (j <- 0 to len_user){
    
           var temp_comm = 0
           if (i != j && (userData(i)._2) == userData(j)._2){
    
              val len_a = userData(i)._1
    
              val len_b = userData(j)._1
    
              var common  = 0;
    
              if (userData(i)._2 == userData(j)._2)
                 common = common + 1
              if (userData(i)._3 == userData(j)._3)
                 common = common + 1
              if (userData(i)._4 == userData(j)._4)
                 common = common + 1
                   
                 temp_comm = temp_comm + common/3.0
           }
           result(len_a)(len_b) = (j, temp_comm)
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
    
    import scala.collection.mutable.Map //avoid misunderstanding we need include this lib

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
