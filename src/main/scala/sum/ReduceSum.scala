/* SimpleApp.scala */
package sum

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType


object ReduceSum {

  def main(args: Array[String]) {


    val f1 = StructField("f1", DataTypes.IntegerType)
    val f2 = StructField("f2", DataTypes.IntegerType)
    val f3 = StructField("f3", DataTypes.IntegerType)
    val f4 = StructField("f4", DataTypes.IntegerType)
    val f5 = StructField("f5", DataTypes.IntegerType)

    val fields = Array(f1, f2, f3, f4, f5)
    val schema = StructType(fields)


    val tupleFile = "/home/harry/Desktop/semester11/thesis/arrow/tuple5.csv" // Should be some file on your system
    val spark = SparkSession.builder.appName("Sum 2 Fields Spark Native").getOrCreate()
    import spark.implicits._
    val tupleData = spark.read
      .schema(schema)
      .csv(tupleFile)
      .cache()

    //tupleData.filter(i => i.get(0).asInstanceOf[Int] < 5).foreach(i=> println(i))

    val startTime = System.nanoTime()
    val sum = tupleData.mapPartitions(tuples => {

      var sum = 0

      tuples.foreach(tuple => {

        sum += tuple.get(0).asInstanceOf[Int]
        sum += tuple.get(1).asInstanceOf[Int]

      })

      Iterator(sum)

    }).cache()

    val counts = sum.count()


    val finalSum = sum.reduce((a, b) => a + b)

    println(finalSum)
    val endTime = System.nanoTime()

    println("Elapsed: " + ((endTime - startTime) / 1000000) + " ms")

    spark.stop()
  }
}
