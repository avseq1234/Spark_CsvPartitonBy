package csv.partition.demo

import org.apache.spark.sql.SparkSession


/**
  * Created by Honda on 2017/11/8.
  */

case class Movie(year:Int,month:Int,name:String,rating:Double)
object CSVPartitionOperator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV PartitionBy")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compress","false")

    val ds = Seq(
      Movie(2012,8,"Batman",9.8),
      Movie(2012,8,"Hero" , 8.7),
      Movie(2011,7,"Git",2.0),
      Movie(2011,6,"Wonder Women" ,10),
      Movie(2015,4,"Iron man",9)
    ).toDS()

    ds.printSchema()
    ds.coalesce(1).write.partitionBy("year","month").csv("csvPartitionTest")
  }
}
