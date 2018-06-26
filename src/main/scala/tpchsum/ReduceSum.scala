/* SimpleApp.scala */
package tpchsum

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType


object ReduceSum {

  def main(args: Array[String]) {


    val f1 = StructField("f1", DataTypes.DoubleType)
    val f2 = StructField("f2", DataTypes.DoubleType)
    val f3 = StructField("f3", DataTypes.DoubleType)
    val f4 = StructField("f4", DataTypes.DoubleType)
    val f5 = StructField("f5", DataTypes.DoubleType)

    val fields = Array(f1, f2, f3, f4, f5)
    val schema = StructType(fields)


    val tupleFile = "/home/harry/Desktop/semester11/thesis/arrow/tuple5_tpch.csv" // Should be some file on your system
    val spark = SparkSession.builder.appName("TPCH sum_charge Spark Native").getOrCreate()
    import spark.implicits._
    val tupleData = spark.read
      .schema(schema)
      .csv(tupleFile)
      .cache()

    //tupleData.filter(i => i.get(0).asInstanceOf[Int] < 5).foreach(i=> println(i))

    val startTime = System.nanoTime()
    val sum = tupleData.mapPartitions(tuples => {

      var sum = 0.0
      var sum1 = 0.0
      var sum2 = 0.0

      tuples.foreach(tuple => {
        // sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        sum += (tuple.get(0).asInstanceOf[Double] * (1 - tuple.get(1).asInstanceOf[Double]) * (1 + tuple.get(2).asInstanceOf[Double]))


        //sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum1+= (tuple.get(0).asInstanceOf[Double] * (1-  tuple.get(1).asInstanceOf[Double]))

        //sum(l_quantity) as sum_qty,
        sum2+= tuple.get(0).asInstanceOf[Double]

      })

      Iterator(Tuple3(sum, sum1, sum2))

    }).cache()

    val counts = sum.count()

    val finalSum = sum.reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))

    println(finalSum)
    val endTime = System.nanoTime()

    println("Elapsed: " + ((endTime - startTime) / 1000000) + " ms")

    spark.stop()
  }
}
