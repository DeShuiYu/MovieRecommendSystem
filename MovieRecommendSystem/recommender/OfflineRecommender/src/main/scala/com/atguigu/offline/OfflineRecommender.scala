package com.atguigu.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String,language: String, genres: String, actors: String, directors: String )
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)
case class MongoConfig(uri:String, db:String)
// 标准推荐对象，mid,score
case class Recommendation(mid: Int, score:Double)
// 用户推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])
// 电影相似度（电影推荐）
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object OfflineRecommender {
  // 定义常量
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"
  // 推荐表的名称
  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    // 定义配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建 spark session
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    //读取 mongoDB 中的业务数据
    val ratingRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating=> (rating.uid, rating.mid, rating.score)).cache()

    //用户的数据集 RDD[Int]
    val userRDD = ratingRDD.map(_._1).distinct()


    //电影数据集 RDD[Int]
    val movieRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid).cache()

    //创建训练数据集
    val trainData = ratingRDD.map(x => Rating(x._1,x._2,x._3))

    // rank 是模型中隐语义因子的个数, iterations 是迭代的次数, lambda 是 ALS 的正则化参
    val (rank,iterations,lambda) = (50, 5, 0.01)
    // 调用 ALS 算法训练隐语义模型
    val model = ALS.train(trainData,rank,iterations,lambda)

    //计算用户推荐矩阵
    val userMovies = userRDD.cartesian(movieRDD)
    // model 已训练好，把 id 传进去就可以得到预测评分列表 RDD[Rating] (uid,mid,rating)
    val preRatings = model.predict(userMovies)

    val userRecs = preRatings
      .filter(_.rating > 0)
      .map(rating => (rating.user,(rating.product, rating.rating)))
      .groupByKey()
      .map{
        case (uid,recs) => UserRecs(uid,recs.toList.sortWith(_._2 >
          _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1,x._2)))
      }.toDF()

    userRecs.write
      .option("uri",mongoConfig.uri)
      .option("collection",USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //TODO：计算电影相似度矩阵

    //计算电影相似度矩阵
    //获取电影的特征矩阵，数据格式 RDD[(scala.Int, scala.Array[scala.Double])]
    val movieFeatures = model.productFeatures.map{case (mid,features) =>
      (mid, new DoubleMatrix(features))
    }
    // 计算笛卡尔积并过滤合并
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{case (a,b) => a._1 != b._1}
      .map{case (a,b) =>
        val simScore = this.consinSim(a._2,b._2) // 求余弦相似度
        (a._1,(b._1,simScore))
      }.filter(_._2._2 > 0.6)
      .groupByKey()
      .map{case (mid,items) =>
        MovieRecs(mid,items.toList.map(x => Recommendation(x._1,x._2)))
      }.toDF()
    movieRecs
      .write
      .option("uri", mongoConfig.uri)
      .option("collection",MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    // 关闭 spark
    spark.stop()
  }

  //计算两个电影之间的余弦相似度
  def consinSim(movie1: DoubleMatrix, movie2:DoubleMatrix) : Double ={
    movie1.dot(movie2) / ( movie1.norm2() * movie2.norm2() )
  }
}
