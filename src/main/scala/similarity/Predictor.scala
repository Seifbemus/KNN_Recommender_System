package similarity

import knn.Predictor.{test, train}
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
  /////////////////////////////////////////////////////////////////////////


  //returns the global average
  def global_avg(): Double = {
    train.map(k => k.rating).reduce(_ + _) / train.count().toDouble
  }

  val global_average = global_avg()

  //returns an RDD[(user, user_average)]
  def RDD_usr_avg_rating(): RDD[(Int, Double)] = {
    val usr_ratings = train.groupBy(x => x.user).map(x => (x._1, x._2.map(y => y.rating)))
    for (usr <- usr_ratings;
         ratings = usr._2;
         avg = if (ratings.isEmpty == false) ratings.reduce(_ + _) / ratings.size else global_average
         ) yield (usr._1, avg)
  }

  val usr_avg_rating = RDD_usr_avg_rating()

  def scale(x: Double, y: Double): Double = {
    if (x > y) 5.0 - y else if (x < y) y - 1.0 else 1.0
  }

  //returns RDD [(user, item , normalized_deviation)]
  def usr_item_norm_dev(): RDD[(Int, Int, Double)] = {
    train.groupBy(x => x.user)
      .leftOuterJoin(usr_avg_rating)
      .mapValues(k => (k._1, k._2 match {
        case None => global_average;
        case Some(a) => a;
      }))
      .flatMap(x => x._2._1.map(y => (y.user, y.item, (y.rating - x._2._2) / scale(y.rating, x._2._2))))

  }

  val user_item_norm_dev = usr_item_norm_dev()


//compute the denominator of eq4 for each user
  def root_squared_sum(): RDD[(Int, Double)] = {
    user_item_norm_dev.groupBy(x => x._1).mapValues(x => math.sqrt((x.map(y => math.pow(y._3, 2))).sum))
  }

  val denominateur = root_squared_sum()

  ///returns RDD [(user , item , preprocessed_rate)]
  def preprocess(): RDD[(Int, Int, Double)] = {
    user_item_norm_dev.groupBy(x => x._1)
      .leftOuterJoin(denominateur)
      .mapValues(x => (x._1, x._2 match {
        case None => 0.0
        case Some(a) => a
      }))
      .flatMap(x => x._2._1.map(y => (y._1, y._2, y._3, x._2._2)))
      .map(x => (x._1, x._2, x._3 / x._4))

  }

  val preprocessed = preprocess()

//compute the cosine similarity between two maps item -> processed rate for two users.
  def cosine_similarity(map1: Map[Int, Double], map2: Map[Int, Double]) = {
    map1.keys.toSet.intersect(map2.keys.toSet).toList
      .map(item => map1(item) * map2(item)).sum
  }

  //compute the jaccard similarity between two maps item -> processed rate for two users.
  def jaccard_similarity(map1: Map[Int, Double], map2: Map[Int, Double]) = {
    map1.keys.toSet.intersect(map2.keys.toSet).size.toDouble / map1.keys.toSet.union(map2.keys.toSet).size.toDouble
  }

  //compute the similarity of a given user with all other users without storing 0 values.
  def compute_similarity(map_user1: (Int, Map[Int, Double]), preprocessed_by_user: List[(Int, Map[Int, Double])], similarity_function: (Map[Int, Double], Map[Int, Double]) => Double) = {
    (map_user1._1, preprocessed_by_user
      .map(y => {
        if (map_user1._1 <= y._1) (y._1, similarity_function(y._2, map_user1._2))
        else (y._1, 0.0)
      }).filter(x => x._2 != 0.0).toMap)
  }

  //compute the similarity of a given user with all other users with storing 0 values.
  def compute_similarity_worst(map_user1: (Int, Map[Int, Double]), preprocessed_by_user: List[(Int, Map[Int, Double])], similarity_function: (Map[Int, Double], Map[Int, Double]) => Double) = {
    (map_user1._1, preprocessed_by_user
      .map(y => {
        if (map_user1._1 <= y._1) (y._1, similarity_function(y._2, map_user1._2))
        else (y._1, 5.0)
      }).filter(x => x._2 != 5.0).toMap)
  }

  //MAP [user,MAP[item,rating]
  val preprocessed_by_user = preprocessed.groupBy(_._1)
    .mapValues(x => x.map(y => (y._2, y._3)).toMap)
    .collect()
    .toList

  //create the map containg all the similarities without storing 0 values.
  def create_similarity_mapping(similarity_function: (Map[Int, Double], Map[Int, Double]) => Double): Map[Int, Map[Int, Double]] = {
    preprocessed_by_user
      .map(x => compute_similarity(x, preprocessed_by_user, similarity_function))
      .toMap
  }

  //create the map containg all the similarities with storing 0 values.
  def create_similarity_mapping_worst(similarity_function: (Map[Int, Double], Map[Int, Double]) => Double): Map[Int, Map[Int, Double]] = {
    //MAP [user,MAP[item,rating]
    preprocessed_by_user
      .map(x => compute_similarity_worst(x, preprocessed_by_user, similarity_function))
      .toMap
  }

  val weighted_deviation_avg = user_item_norm_dev.map(x => x._3).sum() / user_item_norm_dev.count()

  //returns RDD[(user,item,test_rating ,usr_avg)]
  val test_with_user_avg = test.map(x => (x.user, x.item, x.rating))
    .groupBy(x => x._1)
    .leftOuterJoin(usr_avg_rating)
    .mapValues(x => (x._1, x._2 match {
      case None => global_average
      case Some(a) => a
    }))
    .flatMap(x => x._2._1.map(y => (y._1, y._2, y._3, x._2._2)))

  //returns RDD[(user, item, test_rating, prediction with cosine)]
  def predict_with_cosine(): RDD[(Int, Int, Double, Double)] = {
    val cosine_similarities = create_similarity_mapping(cosine_similarity)
    val map_items_list_of_users = user_item_norm_dev.groupBy(_._2).mapValues(x => x.map(y => (y._1, y._3))).collect().toMap
    test_with_user_avg.map(y => (y._1, y._2, y._3, y._4, compute_weighted_sum_dev(map_items_list_of_users, y._1, y._2, cosine_similarities)))
      .map(x => {
        (x._1, x._2, x._3, x._4 + x._5 * scale((x._4 + x._5), x._4))
      })
  }

  //returns RDD[(user, item, test_rating, prediction with jaccard)]
  def predict_with_jaccard(): RDD[(Int, Int, Double, Double)] = {
    val jaccard_similarities = create_similarity_mapping(jaccard_similarity)
    val map_items_list_of_users = user_item_norm_dev.groupBy(_._2).mapValues(x => x.map(y => (y._1, y._3))).collect().toMap
    test_with_user_avg.map(y => (y._1, y._2, y._3, y._4, compute_weighted_sum_dev(map_items_list_of_users, y._1, y._2, jaccard_similarities)))
      .map(x => {
        (x._1, x._2, x._3, x._4 + x._5 * scale((x._4 + x._5), x._4))
      })
  }

  //compute the user-specific weighted-sum deviation for a specific item and user
  def compute_weighted_sum_dev(a: Map[Int, Iterable[(Int, Double)]], user: Int, item: Int, similarities: Map[Int, Map[Int, Double]]) = {
    val k = a.get(item)
    k match {
      case None => 0.0
      case Some(a) => {
        val tab = a.map(y => {
          if (user <= y._1) (similarities(user).getOrElse(y._1, 0.0), y._2) else (similarities(y._1).getOrElse(user, 0.0), y._2)
        }).map(y => (y._1 * y._2, math.abs(y._1))).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        if (tab._2 != 0.0) tab._1 / tab._2 else 0.0
      }
    }

  }

  //return the MAE with the test ratings and its corresponding predictions in rdd1
  def MAE(rdd1: RDD[(Int, Int, Double, Double)]) = {
    rdd1.map(x => math.abs(x._3 - x._4)).sum() / test.count().toDouble

  }

  val cosine_MAE = MAE(predict_with_cosine())

  val jaccard_MAE = MAE(predict_with_jaccard())

  var start = 0.0
  var end = 0.0

  val prediction_times = (1 to 5).toList.map(x => {
    start = System.nanoTime()
    val accuracy = MAE(predict_with_cosine())
    end = System.nanoTime()
    0.001 * (end - start)
  })

  val similarities_times = (1 to 5).toList.map(x => {
    start = System.nanoTime()
    val similarities = create_similarity_mapping(cosine_similarity)
    end = System.nanoTime()
    0.001 * (end - start)
  })

  val cosine_similarities = create_similarity_mapping(cosine_similarity)

  val cosine_similarities_worst = create_similarity_mapping_worst(cosine_similarity)

  val nb_similarity_computation_worst = cosine_similarities_worst.map(x => x._2.size).sum

  val nb_similarity_computation = cosine_similarities.map(x => x._2.size).sum

  val nb_multiplications_foreach_sim_worst = {
    val map_preprocessed_by_user = preprocessed_by_user.toMap
    cosine_similarities_worst.flatMap(x => {
      val items1 =
        map_preprocessed_by_user.get(x._1) match {
          case None => Set[Int]()
          case Some(a) => a.keys.toSet
        }
      x._2.map(y => {

        val items2 = map_preprocessed_by_user.get(y._1) match {
          case None => Set[Int]()
          case Some(a) => a.keys.toSet
        }
        items1.intersect(items2).size.toDouble


      })
    }).toList
  }

  val nb_users = 943

  // Save answers as JSON
  def printToFile(content: String,
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach {
      f =>
        try {
          f.write(content)
        } finally {
          f.close
        }
    }

  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q2.3.1" -> Map(
            "CosineBasedMae" -> cosine_MAE, // Datatype of answer: Double
            "CosineMinusBaselineDifference" -> (cosine_MAE - 0.7669) // Datatype of answer: Double
          ),

          "Q2.3.2" -> Map(
            "JaccardMae" -> jaccard_MAE, // Datatype of answer: Double
            "JaccardMinusCosineDifference" -> (jaccard_MAE - cosine_MAE) // Datatype of answer: Double
          ),

          "Q2.3.3" -> Map(
            // Provide the formula that computes the number of similarity computations
            // as a function of U in the report.
            "NumberOfSimilarityComputationsForU1BaseDataset" -> nb_similarity_computation_worst // Datatype of answer: Int
          ),

          "Q2.3.4" -> Map(
            "CosineSimilarityStatistics" -> Map(
              "min" -> {
                nb_multiplications_foreach_sim_worst.min
              }, // Datatype of answer: Double
              "max" -> {
                nb_multiplications_foreach_sim_worst.max
              }, // Datatype of answer: Double
              "average" -> {
                nb_multiplications_foreach_sim_worst.sum / nb_multiplications_foreach_sim_worst.size.toDouble
              }, // Datatype of answer: Double
              "stddev" -> {
                val avg = nb_multiplications_foreach_sim_worst.sum / nb_multiplications_foreach_sim_worst.size.toDouble
                Math.sqrt(nb_multiplications_foreach_sim_worst.map(_ - avg).map(t => t * t).sum / nb_multiplications_foreach_sim_worst.size.toDouble)
              } // Datatype of answer: Double
            )
          ),

          "Q2.3.5" -> Map(
            // Provide the formula that computes the amount of memory for storing all S(u,v)
            // as a function of U in the report.
            "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" -> {
              8 * nb_similarity_computation
            } // Datatype of answer: Int
          ),

          "Q2.3.6" -> Map(
            "DurationInMicrosecForComputingPredictions" -> Map(
              "min" -> {
                prediction_times.min
              }, // Datatype of answer: Double
              "max" -> {
                prediction_times.max
              }, // Datatype of answer: Double
              "average" -> {
                (prediction_times.sum / prediction_times.length.toDouble)
              }, // Datatype of answer: Double
              "stddev" -> {
                (Math.sqrt((prediction_times.map(_ - prediction_times.sum / prediction_times.length.toDouble)
                  .map(t => t * t).sum) / prediction_times.length))
              } // Datatype of answer: Double
            )
            // Discuss about the time difference between the similarity method and the methods
            // from milestone 1 in the report.
          ),

          "Q2.3.7" -> Map(
            "DurationInMicrosecForComputingSimilarities" -> Map(
              "min" -> {
                similarities_times.min
              }, // Datatype of answer: Double
              "max" -> {
                similarities_times.max
              }, // Datatype of answer: Double
              "average" -> {
                (similarities_times.sum / similarities_times.length.toDouble)
              }, // Datatype of answer: Double
              "stddev" -> {
                (Math.sqrt((similarities_times.map(_ - similarities_times.sum / similarities_times.length.toDouble)
                  .map(t => t * t).sum) / similarities_times.length))
              } // Datatype of answer: Double
            ),
            "AverageTimeInMicrosecPerSuv" -> {
              (similarities_times.sum / similarities_times.length.toDouble) / nb_similarity_computation
            }, // Datatype of answer: Double
            "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> {
              (similarities_times.sum / similarities_times.length.toDouble) / (prediction_times.sum / prediction_times.length.toDouble)
            } // Datatype of answer: Double
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
