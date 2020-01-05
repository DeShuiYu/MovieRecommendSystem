package com.atguigu.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
case class MongoConfig(uri:String, db:String)
case class Recommendation(mid:Int, score:Double)
case class GenresRecommendation(genres:String, recs:Seq[Recommendation])

object StatisticsRecommender {
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"
  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //创建 SparkConf 配置
    val sparkConf = new  SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))
    //创建 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    //加入隐式转换
    import spark.implicits._

    //数据加载进来
    val ratingDF = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    //创建一张名叫 ratings 的表
    ratingDF.createOrReplaceTempView("ratings")

    //TODO:不同的统计推荐结果
    //1. 历史热门统计，历史评分数据最多
    val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid")
    rateMoreMoviesDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    //2. 近期热门统计，按照“yyyyMM”格式选取最近的评分数据，统计评分个数

    //创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    //注册一个 UDF 函数，用于将 timestamp 装换成年月格式 1260759144000 => 201605
    spark.udf.register("changeDate",(x:Int) => simpleDateFormat.format(new Date(x *
      1000L)).toInt)
    // 将原来的 Rating 数据集中的时间转换成年月的格式
    val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearmonth  from ratings")
      // 将新的数据集注册成为一张表
      ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")
      val rateMoreRecentlyMovies = spark.sql("select mid, count(mid) as count ,yearmonth from ratingOfMonth group by yearmonth,mid")
      rateMoreRecentlyMovies
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_RECENTLY_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    //3. 优质电影统计，统计电影的平均评分
    val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by   mid")
      averageMoviesDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",AVERAGE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()



    //4. 各类别电影top统计
    val movieWithScore = movieDF.join(averageMoviesDF,Seq("mid"))
    //所有的电影类别
    val genres =
      List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery","Romance","Science","Tv","Thriller","War","Western")
      //将电影类别转换成 RDD
      val genresRDD = spark.sparkContext.makeRDD(genres)
      //计算电影类别 top10
//    genresRDD.cartesian(movieWithScore.rdd).foreach(row=>println("genresRDD",row))
      val genrenTopMovies = genresRDD.cartesian(movieWithScore.rdd)
      .filter{
      case (genres,row) =>
      row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
      }
      .map{
      // 将整个数据集的数据量减小，生成 RDD[String,Iter[mid,avg]]
        case (genres,row) => {
          (genres,(row.getAs[Int]("mid"), row.getAs[Double]("avg")))
        }
      }.groupByKey()
        .map{
          case (genres, items) => GenresRecommendation(genres,items.toList.sortWith(_._2 >
            _._2).take(10).map(item => Recommendation(item._1,item._2)))
        }.toDF()
    // 输出数据到 MongoDB
    genrenTopMovies
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    spark.stop()

  }
}
