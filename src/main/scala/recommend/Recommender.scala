package recommend


import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Recommender extends App {
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
  println("Loading data from: " + conf.data())
  val dataFile = spark.sparkContext.textFile(conf.data())
  val data = dataFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(data.count == 100000, "Invalid data")

  println("Loading personal data from: " + conf.personal())
  val personalFile = spark.sparkContext.textFile(conf.personal())
  // TODO: Extract ratings and movie titles
  assert(personalFile.count == 1682, "Invalid personal data")

  val personal_data = personalFile.map(l => {
    val cols = l.split(",").map(_.trim)
    cols.size match {
      case 3 => Rating(944, cols(0).toInt, cols(2).toDouble)
      case _ => Rating(-1, 0, 0)
    }
  }).filter(x=>(x.user!=(-1)))

  //returns an RDD[(item, item_name)]
  val item_names =personalFile.map(l => {
    val cols = l.split(",").map(_.trim)
    (cols(0).toInt,cols(1).toString)

  })

  //returns an RDD[(item)] containing the rated items
  val rated_items=personal_data.map(x=>x.item)

  //Add my ratings to the data
  val all_data= data.union(personal_data)


  def global_avg(): Double ={
    all_data.map(k=>k.rating).reduce(_+_)/all_data.count().toDouble
  }

  val global_average=global_avg()

  def RDD_usr_avg_rating():RDD[(Int,Double)]= {
    val usr_ratings = all_data.groupBy(x => x.user).map(x => (x._1, x._2.map(y => y.rating)))
    for (usr <- usr_ratings;
         ratings = usr._2;
         avg = if (ratings.isEmpty == false) ratings.reduce(_ + _) / ratings.size else global_average
         ) yield (usr._1, avg)
  }

  val usr_avg_rating=RDD_usr_avg_rating()

  def scale(x :Double, y:Double ):Double={
    if (x>y) 5.0-y else if (x<y) y-1.0 else 1.0
  }

  def usr_item_norm_dev():RDD[(Int,Int,Double)]={
    all_data.groupBy(x=>x.user)
      .leftOuterJoin(usr_avg_rating)
      .mapValues(k=>(k._1,k._2 match {
        case None =>  global_average;
        case Some(a) => a ;
      }))
      .flatMap(x=>x._2._1.map(y=>(y.user,y.item,(y.rating-x._2._2)/scale(y.rating,x._2._2) )))

  }

  val user_item_norm_dev=usr_item_norm_dev()


  ///returns RDD [(user , denominator)]
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

  val non_rated_items=all_data.groupBy(x=>x.item).map(x=>x._1).subtract(rated_items)

  val test_with_user_avg= non_rated_items
    .map(x=>(944,x))
    .groupBy(x => x._1)
    .leftOuterJoin(usr_avg_rating)
    .mapValues(x => (x._1, x._2 match {
      case None => global_average
      case Some(a) => a
    }))
    .flatMap(x => x._2._1.map(y => (y._1, y._2, x._2._2)))


  def compute_weighted_sum_dev(a: Map[Int, Iterable[(Int, Double)]] , user:Int ,item:Int,similarities: Map[Int, Map[Int, Double]]):Double= {
    a.get(item) match {
      case None => 0.0
      case Some(a) => {
        val tab = a.map(y => (similarities(user).getOrElse(y._1, 0.0), y._2)).map(y => (y._1 * y._2, math.abs(y._1))).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        if (tab._2 != 0.0) tab._1 / tab._2 else 0.0
      }
    }
  }


  def predict_with_cosine(k:Int):RDD[(Int,Double)]= {
    val k_cosine_similarities=create_similarity_mapping(cosine_similarity, k )
    val map_items_list_of_users=user_item_norm_dev.groupBy(_._2).mapValues(x=>x.map(y=>(y._1,y._3))).collect().toMap
    test_with_user_avg.map(y=> (y._1, y._2, y._3 , compute_weighted_sum_dev(map_items_list_of_users, y._1,y._2, k_cosine_similarities)))
      .map(x => {
        ( x._2, x._3 + x._4 * scale((x._3 + x._4), x._3))
      })
  }



    //extract the 5 best predictions.
    val predictions_with_30=predict_with_cosine(30).leftOuterJoin(item_names).mapValues(x=>(x._1,x._2 match {
      case None => ""
      case Some(a) => a
    })).map(x=>(x._1,x._2._2,x._2._1))
    val oredered_predictions_30=predictions_with_30.groupBy(x=>x._3).mapValues(x=>x.toList.sortBy(_._1)).lookup(5).head



  val predictions_with_300=predict_with_cosine(300).leftOuterJoin(item_names).mapValues(x=>(x._1,x._2 match {
    case None => ""
    case Some(a) => a
  })).map(x=>(x._1,x._2._2,x._2._1))
  val oredered_predictions_300=predictions_with_300.groupBy(x=>x._3).mapValues(x=>x.toList.sortBy(_._1)).lookup(5).head



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

          // IMPORTANT: To break ties and ensure reproducibility of results,
          // please report the top-5 recommendations that have the smallest
          // movie identifier.

          "Q3.2.5" -> Map(
            "Top5WithK=30" ->
              List[Any](
                List(oredered_predictions_30(0)._1,oredered_predictions_30(0)._2 , oredered_predictions_30(0)._3), // Datatypes for answer: Int, String, Double
                List(oredered_predictions_30(1)._1,oredered_predictions_30(1)._2 , oredered_predictions_30(1)._3), // Representing: Movie Id, Movie Title, Predicted Rating
                List(oredered_predictions_30(2)._1,oredered_predictions_30(2)._2 , oredered_predictions_30(2)._3), // respectively
                List(oredered_predictions_30(3)._1,oredered_predictions_30(3)._2 , oredered_predictions_30(3)._3),
                List(oredered_predictions_30(4)._1,oredered_predictions_30(4)._2 , oredered_predictions_30(4)._3)

              ),

            "Top5WithK=300" ->
              List[Any](
                List(oredered_predictions_300(0)._1,oredered_predictions_300(0)._2 , oredered_predictions_300(0)._3), // Datatypes for answer: Int, String, Double
                List(oredered_predictions_300(1)._1,oredered_predictions_300(1)._2 , oredered_predictions_300(1)._3), // Representing: Movie Id, Movie Title, Predicted Rating
                List(oredered_predictions_300(2)._1,oredered_predictions_300(2)._2 , oredered_predictions_300(2)._3), // respectively
                List(oredered_predictions_300(3)._1,oredered_predictions_300(3)._2 , oredered_predictions_300(3)._3),
                List(oredered_predictions_300(4)._1,oredered_predictions_300(4)._2 , oredered_predictions_300(4)._3)
              )

            // Discuss the differences in rating depending on value of k in the report.
          )
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
