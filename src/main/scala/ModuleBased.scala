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
http://www.csdn.net/article/2015-05-07/2824641
*/
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.io.Source

object ModuleBased {

    //用最佳模型预测测试集的评分，并计算和实际评分之间的均方根误差（RMSE�?
    // val testRmse = computeRmse(bestModel.get, test, numTest)
    //    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
    //   + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")
    //create a naive baseline and compare it with the best model
   //val tmpData = loadRatings("/home/sherry/web-data/test.csv")
   //val testData = sc.parallelize(tmpData)    

  /** 校验集预测数据和实际数据之间的均方根误差 **/
  def train(ratings: RDD[Rating],num:Int):Array[Rating]{
      //count the toaltal rating
    val numRatings = ratings.count()
    //val numUsers = ratings.map(_.user).distinct().count()
    val numUsers = ratings.map(_.user).collect.distinct.length
    //val numEvents = ratings.map(_.product).distinct().count()
    val numEvents = ratings.map(_.product).collect.distinct.length

    println("Got " + numRatings + " ratings from " + numUsers + " users " + numEvents + " Events")  
    //将样本评分表以key值切分成3个部分，分别用于训练 (60%，并加入用户评分), 校验 (20%), and 测试 (20%)
    //该数据在计算过程中要多次应用到，所以cache到内�?
    val numPartitions = 4
    
    val training = sc.makeRDD(ratings.collect.dropRight((numRatings*0.4).toInt)) 
    
    val validation = sc.parallelize(ratings.collect.drop((numRatings*0.fg6)
                .toInt).dropRight((numRatings*0.2).toInt))
    val test = sc.parallelize(ratings.collect.drop((numRatings*0.9).toInt)).persist()
    // to valify whether the
    val numTraining = training.count()

    val numValidation = validation.count()

    val numTest = test.count()

    println("Training: " + numTraining + " validation: " + numValidation + " test: " + numTest)
    //训练不同参数下的模型，并在校验集中验证，获取最佳参数下的模�?
    val ranks = List(8, 12)

    val lambdas = List(0.1, 10.0)

    val numIters = List(10, 20)

    var bestModel: Option[MatrixFactorizationModel] = None

    var bestValidationRmse = Double.MaxValue

    var bestRank = 0

    var bestLambda = -1.0

    var bestNumIter = -1

    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
    
        val model =ALS.train(ratings, rank, numIter, lambda)
        //val model = ALS.train(ratings, 8, 10, 0.1)
        val validationRmse = computeRmse(model, validation, numValidation)
    
           println("validationRmse: "+validationRmse+ "bestValidationRmse"+bestValidationRmse)
    
         if (validationRmse < bestValidationRmse) {
           println("validationRmse"+validationRmse)
          bestModel = Some(model)
    
          val testRmse = computeRmse(bestModel.get, test, numTest)
                     println("test: "+testRmse)
    
          bestValidationRmse = validationRmse
    
          bestRank = rank
    
          bestLambda = lambda
    
          bestNumIter = numIter
        }
    }
   val testUser = test.map(_.user).distinct()
   
   val allUser = ratings.map(_.user).distinct()
   
   val result = allUser.union(testUser).collect.flatMap { user =>
       bestModel.get.recommendProducts(user, num)}

       return result
  }//END OF THE FUCTION
 
  def computeRmse(model: MatrixFactorizationModel,data:RDD[Rating],n:Long):Double = {
    println("==========here============="+data.count)
    val usersProducts= data.map {case Rating(user, product, rate) =>(user, product)}
    println(usersProducts.count)
    var predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>((user, product), rate)}
    println(predictions.count)
    val ratesAndPreds = data.map { case Rating(user, product, rate) =>((user, product), rate)}.join(predictions)
    println(ratesAndPreds.count)
//=====end of test  
    return  math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
                  val err = (r1 - r2)
                  err * err}.mean())
  }//end of the compute function

   // def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
   //    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
   //    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
   //      .join(data.map(x => ((x.user, x.product), x.rating)))
   //      .values
   //    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
   //  }

  def loadRatings(path:String):Seq[Rating] = {
    val lines = Source.fromFile(path).getLines()

    val ratings = lines.map{

      lines =>

        val fields = lines.split(",")

        var userId = fields(0).toLong
        
        var eventId = fields(1).toLong
        
        if( userId > 2147483646) userId = userId %2147483646
        
        if( eventId > 2147483646) eventId = eventId %2147483646 
        
        Rating(userId.toInt, eventId.toInt, fields(2).toDouble)
    }.toSeq

    if(ratings.isEmpty){

      sys.error("No ratings provided.")

    }else{
      return ratings
    }

  }//end of the function


}// end of  the object 

//=================================

