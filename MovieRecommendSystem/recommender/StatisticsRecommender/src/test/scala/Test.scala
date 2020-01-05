import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val rdd1 = spark.sparkContext.makeRDD(List("1","3","5"))
    val rdd2 = spark.sparkContext.makeRDD(List("2","4","6"))
//    result
//    (1,2)
//    (1,4)
//    (1,6)
//    (3,2)
//    (3,4)
//    (3,6)
//    (5,2)
//    (5,4)
//    (5,6)


    rdd1.cartesian(rdd2).foreach(println)

    spark.stop()


  }
}
