package knn

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Predictor extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("")
  println("******************************************************")

  var conf = new Conf(args)
  println("Loading training data from: " + conf.train())
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(test.count == 20000, "Invalid test data")
  // INSERT CODE HERE

  //returns the global average
  def global_avg(): Double ={
    train.map(k=>k.rating).reduce(_+_)/train.count().toDouble
  }

  val global_average=global_avg()

  //returns an RDD[(user, user_average)]
  def RDD_usr_avg_rating():RDD[(Int,Double)]= {
    val usr_ratings = train.groupBy(x => x.user).map(x => (x._1, x._2.map(y => y.rating)))
    for (usr <- usr_ratings;
         ratings = usr._2;
         avg = if (ratings.isEmpty == false) ratings.reduce(_ + _) / ratings.size else global_average
         ) yield (usr._1, avg)
  }

  val usr_avg_rating=RDD_usr_avg_rating()

  def scale(x :Double, y:Double ):Double={
    if (x>y) 5.0-y else if (x<y) y-1.0 else 1.0
  }

  //returns RDD [(user, item , normalized_deviation)]
  def usr_item_norm_dev():RDD[(Int,Int,Double)]={
    train.groupBy(x=>x.user)
      .leftOuterJoin(usr_avg_rating)
      .mapValues(k=>(k._1,k._2 match {
        case None =>  global_average;
        case Some(a) => a ;
      }))
      .flatMap(x=>x._2._1.map(y=>(y.user,y.item,(y.rating-x._2._2)/scale(y.rating,x._2._2) )))

  }

  val user_item_norm_dev=usr_item_norm_dev()


  /////compute the denominator of eq4 for each user :returns RDD [(user , denominator)]
  def root_squared_sum():RDD[(Int,Double)]={
    user_item_norm_dev.groupBy(x=>x._1).mapValues(x=>math.sqrt((x.map(y=>math.pow(y._3,2))).sum))
  }


  ///returns RDD [(user , item , preprocessed_rate)]
  def preprocess():RDD[(Int,Int,Double)]={
    val denominateur=root_squared_sum()
    user_item_norm_dev.groupBy(x=>x._1)
      .leftOuterJoin(denominateur)
      .mapValues(x=>(x._1,x._2 match {
        case None => global_average
        case Some(a) => a
      }))
      .flatMap(x=>x._2._1.map(y=>(y._1,y._2,y._3,x._2._2)))
      .map(x=>(x._1,x._2,x._3/x._4))

  }

  val preprocessed=preprocess()

  //create the map containg all the similarities without storing 0 values.
  def create_similarity_mapping(similarity_function :(Map[Int, Double], Map[Int, Double]) => Double,k:Int):Map[Int,Map[Int,Double]]= {
    //MAP [user,MAP[item,rating]
    val users_items_map = preprocessed.groupBy(_._1)
      .mapValues(x => x.map(y => (y._2, y._3)).toMap)
      .collect()
      .toMap
    users_items_map
      .map(x=>compute_similarity(x,users_items_map,similarity_function,k))
  }


  //compute the similarity of a given user with all other users without storing 0 values.
  def compute_similarity(user1_items_map:(Int, Map[Int, Double]),users_items_map:Map[Int, Map[Int, Double]],similarity_function:(Map[Int, Double], Map[Int, Double]) => Double,k:Int)={
    (user1_items_map._1,users_items_map.filter(x=>x._1!=user1_items_map._1)
      .mapValues(y=>similarity_function(y,user1_items_map._2)).toList.sortBy(_._2)(Ordering[Double].reverse).take(k).toMap)
  }

  //compute the cosine similarity between two maps item -> processed rate for two users.
  def cosine_similarity(items_map1: Map [Int, Double],items_map2: Map [Int, Double]) ={
    items_map1.keys.toSet.intersect(items_map2.keys.toSet).toList
      .map(item=>items_map1(item)*items_map2(item)).sum
  }


  val weighted_deviation_avg=user_item_norm_dev.map(x=>x._3).sum()/user_item_norm_dev.count()

  //returns RDD[(user,item,test_rating ,usr_avg)]
  val test_with_user_avg= test.map(x => (x.user, x.item, x.rating))
    .groupBy(x => x._1)
    .leftOuterJoin(usr_avg_rating)
    .mapValues(x => (x._1, x._2 match {
      case None => global_average
      case Some(a) => a
    }))
    .flatMap(x => x._2._1.map(y => (y._1, y._2, y._3, x._2._2)))


  //returns RDD[(user, item, test_rating, prediction with cosine)]
  def predict_with_cosine(k:Int):RDD[(Int,Int,Double,Double)]= {
    val k_cosine_similarities=create_similarity_mapping(cosine_similarity ,k)
    val map_items_list_of_users=user_item_norm_dev.groupBy(_._2).mapValues(x=>x.map(y=>(y._1,y._3))).collect().toMap
    test_with_user_avg.map(y=> (y._1, y._2, y._3, y._4, compute_weighted_sum_dev(map_items_list_of_users, y._1,y._2, k_cosine_similarities)))
    .map(x => {
      (x._1, x._2, x._3, x._4 + x._5 * scale((x._4 + x._5), x._4))
    })
  }

  //compute the user-specific weighted-sum deviation for a specific item and user
  def compute_weighted_sum_dev(a: Map[Int, Iterable[(Int, Double)]] , user:Int ,item:Int,similarities: Map[Int, Map[Int, Double]])={
      a.get(item) match {
      case None => 0.0
      case Some(a) => {
        val tab=a.map(y=>(similarities(user).getOrElse(y._1,0.0),y._2)).map(y=>(y._1*y._2 , math.abs(y._1))).reduce((x,y)=>(x._1+y._1,x._2+y._2))
        if (tab._2!=0.0) tab._1/tab._2 else 0.0
      }
    }


  }


  //return the MAE with the test ratings and its corresponding predictions in rdd1
  def MAE(rdd1:RDD[(Int,Int,Double,Double)])= {
    rdd1.map(x => math.abs(x._3 - x._4)).sum() / test.count().toDouble

  }

  val predictions_with_k:Map[Int,Double]=List((10,MAE(predict_with_cosine(10))),(30,MAE(predict_with_cosine(30))),(50,MAE(predict_with_cosine(50))),(100,MAE(predict_with_cosine(100))),(200,MAE(predict_with_cosine(200))),
    (300,MAE(predict_with_cosine(300))),(400,MAE(predict_with_cosine(400))),(800,MAE(predict_with_cosine(800))),(943,MAE(predict_with_cosine(943)))).toMap



  // Save answers as JSON
  def printToFile(content: String,
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q3.2.1" -> Map(
            // Discuss the impact of varying k on prediction accuracy on
            // the report.
            "MaeForK=10" -> predictions_with_k.get(10), // Datatype of answer: Double
            "MaeForK=30" -> predictions_with_k.get(30), // Datatype of answer: Double
            "MaeForK=50" -> predictions_with_k.get(50), // Datatype of answer: Double
            "MaeForK=100" -> predictions_with_k.get(100), // Datatype of answer: Double
            "MaeForK=200" -> predictions_with_k.get(200), // Datatype of answer: Double
            "MaeForK=300" -> predictions_with_k.get(300), // Datatype of answer: Double
            "MaeForK=400" -> predictions_with_k.get(400), // Datatype of answer: Double
            "MaeForK=800" -> predictions_with_k.get(800), // Datatype of answer: Double
            "MaeForK=943" -> predictions_with_k.get(943), // Datatype of answer: Double
            "LowestKWithBetterMaeThanBaseline" -> predictions_with_k.toList.filter(x=>x._2<0.7669).sortBy(_._2)(Ordering[Double].reverse).head._1, // Datatype of answer: Int
            "LowestKMaeMinusBaselineMae" -> (predictions_with_k.toList.filter(x=>x._2<0.7669).sortBy(_._2)(Ordering[Double].reverse).head._2-0.7669) // Datatype of answer: Double
          ),

          "Q3.2.2" ->  Map(
            // Provide the formula the computes the minimum number of bytes required,
            // as a function of the size U in the report.
            "MinNumberOfBytesForK=10" -> 8*create_similarity_mapping(cosine_similarity ,10).map(x=>x._2.size).sum, // Datatype of answer: Int
            "MinNumberOfBytesForK=30" -> 8*create_similarity_mapping(cosine_similarity ,30).map(x=>x._2.size).sum, // Datatype of answer: Int
            "MinNumberOfBytesForK=50" -> 8*create_similarity_mapping(cosine_similarity ,50).map(x=>x._2.size).sum, // Datatype of answer: Int
            "MinNumberOfBytesForK=100" -> 8*create_similarity_mapping(cosine_similarity ,100).map(x=>x._2.size).sum, // Datatype of answer: Int
            "MinNumberOfBytesForK=200" -> 8*create_similarity_mapping(cosine_similarity ,200).map(x=>x._2.size).sum, // Datatype of answer: Int
            "MinNumberOfBytesForK=300" -> 8*create_similarity_mapping(cosine_similarity ,300).map(x=>x._2.size).sum, // Datatype of answer: Int
            "MinNumberOfBytesForK=400" -> 8*create_similarity_mapping(cosine_similarity  ,400).map(x=>x._2.size).sum, // Datatype of answer: Int
            "MinNumberOfBytesForK=800" -> 8*create_similarity_mapping(cosine_similarity,800).map(x=>x._2.size).sum, // Datatype of answer: Int
            "MinNumberOfBytesForK=943" -> 8*create_similarity_mapping(cosine_similarity ,943).map(x=>x._2.size).sum // Datatype of answer: Int
          ),

          "Q3.2.3" -> Map(
            "SizeOfRamInBytes" -> 4e9, // Datatype of answer: Int
            "MaximumNumberOfUsersThatCanFitInRam" -> 4e9/predictions_with_k.toList.filter(x=>x._2<0.7669).sortBy(_._2)(Ordering[Double].reverse).head._1*3*8 // Datatype of answer: Int
          )

          // Answer the Question 3.2.4 exclusively on the report.
         )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
