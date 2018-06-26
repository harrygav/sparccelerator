/* SimpleApp.scala */
package math

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType


object Reduce3 {

  def main(args: Array[String]) {


    val f1 = StructField("f1", DataTypes.DoubleType)
    val f2 = StructField("f2", DataTypes.DoubleType)
    val f3 = StructField("f3", DataTypes.DoubleType)
    val f4 = StructField("f4", DataTypes.DoubleType)
    val f5 = StructField("f5", DataTypes.DoubleType)

    val fields = Array(f1, f2, f3)
    val schema = StructType(fields)


    //case class MyTuple5(f1: Int, f2: Int, f3: Int, f4: Int, f5: Int)


    val tupleFile = "/home/harry/Desktop/semester11/thesis/arrow/3vector.csv" // Should be some file on your system
    val spark = SparkSession.builder.appName("Spark Native minmax 3 cols").getOrCreate()

    import spark.implicits._
    val tupleData = spark.read
      .schema(schema)
      .csv(tupleFile)
      .cache()

    //tupleData.filter(i => i.get(0).asInstanceOf[Int] < 5).foreach(i=> println(i))

    val startTime = System.nanoTime()

    val minmaxs = tupleData.mapPartitions(tuples => {

      var min1 = Integer.MAX_VALUE.toDouble
      var max1 = Integer.MIN_VALUE.toDouble

      var min2 = Integer.MAX_VALUE.toDouble
      var max2 = Integer.MIN_VALUE.toDouble

      var min3 = Integer.MAX_VALUE.toDouble
      var max3 = Integer.MIN_VALUE.toDouble

      tuples.foreach(tuple => {

        if (tuple.get(0).asInstanceOf[Double] > max1)
          max1 = tuple.get(0).asInstanceOf[Double]
        else if (tuple.get(0).asInstanceOf[Double] < min1)
          min1 = tuple.get(0).asInstanceOf[Double]

        if (tuple.get(1).asInstanceOf[Double] > max2)
          max2 = tuple.get(1).asInstanceOf[Double]
        else if (tuple.get(1).asInstanceOf[Double] < min2)
          min2 = tuple.get(1).asInstanceOf[Double]

        if (tuple.get(2).asInstanceOf[Double] > max3)
          max3 = tuple.get(2).asInstanceOf[Double]
        else if (tuple.get(2).asInstanceOf[Double] < min3)
          min3 = tuple.get(2).asInstanceOf[Double]

      })

      val res = (min1, max1, min2, max2, min3, max3)
      Iterator(res)

    }).cache()

    val tempRes = minmaxs.collect()

    val finalRes = (
      tempRes.minBy(_._1)._1, tempRes.maxBy(_._2)._2,
      tempRes.minBy(_._3)._3, tempRes.maxBy(_._4)._4,
      tempRes.minBy(_._5)._5, tempRes.maxBy(_._6)._6
    )

    println(finalRes)
    val endTime = System.nanoTime()

    println("Elapsed: " + ((endTime - startTime) / 1000000) + " ms")
    spark.stop()
  }
}
