/* SimpleApp.scala */
package math

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer


object Reduce {

  def main(args: Array[String]) {


    val f1 = StructField("f1", DataTypes.DoubleType)
    val f2 = StructField("f2", DataTypes.DoubleType)
    val f3 = StructField("f3", DataTypes.DoubleType)
    val f4 = StructField("f4", DataTypes.DoubleType)
    val f5 = StructField("f5", DataTypes.DoubleType)

    val fields = Array(f1)
    val schema = StructType(fields)


    //case class MyTuple5(f1: Int, f2: Int, f3: Int, f4: Int, f5: Int)


    val tupleFile = "/home/harry/Desktop/semester11/thesis/arrow/vector.csv" // Should be some file on your system
    val spark = SparkSession.builder.appName("Spark Native minmax").getOrCreate()

    import spark.implicits._
    val tupleData = spark.read
      .schema(schema)
      .csv(tupleFile)
      .cache()

    //tupleData.filter(i => i.get(0).asInstanceOf[Int] < 5).foreach(i=> println(i))

    val startTime = System.nanoTime()

    val minmaxs = tupleData.mapPartitions(tuples => {

      var min = Integer.MAX_VALUE.toDouble
      var max = Integer.MIN_VALUE.toDouble

      tuples.foreach(tuple => {

        if (tuple.get(0).asInstanceOf[Double] > max)
          max = tuple.get(0).asInstanceOf[Double]
        else if (tuple.get(0).asInstanceOf[Double] < min)
          min = tuple.get(0).asInstanceOf[Double]

      })

      val res = (min, max)
      Iterator(res)

    }).cache()

    val tempRes = minmaxs.collect()

    val finalRes = (tempRes.minBy(_._1), tempRes.maxBy(_._2))._1

    println(finalRes)
    val endTime = System.nanoTime()

    println("Elapsed: " + ((endTime - startTime) / 1000000) + " ms")
    spark.stop()
  }
}
