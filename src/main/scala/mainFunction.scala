package scala

import org.apache.log4j.{Level, Logger}

import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql._

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

import org.apache.spark.rdd._

import org.apache.spark.SparkContext._

import scala._

import java.io.File

import scala.collection.mutable.Map

import Array._

object mainFunction {

  def main(args: Array[String]) {
    // Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    // Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    println("=====================step 1 initial conf==========================")
    //³õÊ¼»¯ÅäÖÃ
    val sparkConf = new SparkConf()

      .setMaster("spark://localhost:7077")

      .setAppName("mainFunction")

      .set("spark.akka.frameSize", "2000")

      .set("spark.network.timeout", "1200")

    val sparkContext = new SparkContext(sparkConf).setMaster("local[5]")

    //hbaseConf.set("hbase.zookeeper.quorum", "cloud4,cloud5,cloud6")
    //hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    //hbaseConf.set("zookeeper.session.timeout", "6000000")

   println("\n=====================step 2 load data==========================")

   val eventHomeDir = "/home/sherry/web-data/train.csv" //args(0)  val eventHomeDir = "/sherry"
    //装载样本评分数据-the this is for the ModuleBased
    val ratings = sc.textFile(eventHomeDir).map {

      line =>

        val fields = line.split(",")
        // now format: user, event, rating
        var grade = 0.0;

        val eventR = (fields(2).toInt, fields(4).toInt, fields(5).toInt)

        eventR match{
          case (1,1,0) => grade = 5.0

          case (0,1,0) => grade = 4.0

          case (1,0,0) => grade = 3.0

          case (0,0,0) => grade = 2.0

          case (1,0,1) => grade = 1.0

          case (0,0,1) => grade = 0.0
        }

        var userId = fields(0).toLong

        var eventId = fields(1).toLong

        if( userId > 2147483646) userId = userId %2147483646

        if( eventId > 2147483646) eventId = eventId %2147483646 

        Rating(userId.toInt, eventId.toInt, grade)
    }

    val data = sc.textFile(eventHomeDir)//args(0)

    var parsedData = data.map(s => (s.split(',')(0).toString, s.split(',')(1).toString,
      s.split(',')(2).toString, s.split(',')(4).toString, s.split(',')(5).toString)).collect
     
    println("\n[Part1]counting the first part----UserBased recommendation")
    
    //val test_parsedData = parsedData.drop((parsedData.size*0.2).toInt)

    var SingleUser = UserBased.UserAction(parsedData) 
             
    var userData = new Array[(Int,String,String,String,String)](0)

    var userMax:Map[String,List[(Int,Long)]] = Map()

    for (s <- SingleUser){

        if(!userMax.contains(s._1)){
            // the List of user-id-event              
              val temp_list = s._2.map(x => (x._1.toInt,x._2.toLong)).distinct         
              userMax += (s._1 -> temp_list)
        }
        val temp_array = concat(s._2.toArray, userData)

        userData = temp_array
    }// the map from username to maxindex
    val similarity_Max = UserBased.UserSimilarity(userData,SingleUser.size)
    
    val result_user = UserBased.UserRecommend(similarity_Max, 3, userMax.toArray)
    
    
     
    println("\n[Part2]counting the second part----ModuleBased recommendation")
    
    println("\n[waiting]It depends on the scale...")
    
    val recomModule_Max = ModuleBased.train(ratings,3)

    println("\n[Part3]counting the third part----ItemBased recommendation")
    
    println("\n[Test]=======It's time to test======")
    val test_user = test()

    
    //µÚÒ»´ÎÔËÐÐ£¬³õÊ¼»¯ÓÃ»§µÄÍÆ¼öÐÅÏ¢
    if (args.length != 0) {
      println("\n=====================system comprehensive...==========================")
      println("\n[DEBUG]training model...")
    
    sparkContext.stop()
  }
}
