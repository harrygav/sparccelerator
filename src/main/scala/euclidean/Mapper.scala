/* SimpleApp.scala */
package euclidean

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType


object Mapper {

  def main(args: Array[String]) {


    val f1 = StructField("f1", DataTypes.IntegerType)
    val f2 = StructField("f2", DataTypes.IntegerType)
    val f3 = StructField("f3", DataTypes.IntegerType)
    val f4 = StructField("f4", DataTypes.IntegerType)
    val f5 = StructField("f5", DataTypes.IntegerType)
    val f6 = StructField("f6", DataTypes.DoubleType)

    val fields = Array(f1, f2, f3, f4, f5)
    val schema = StructType(fields)
    val schema6 = StructType(fields ++ Array(f6))

    val encoder = RowEncoder(schema6)

    val tupleFile = "/home/harry/Desktop/semester11/thesis/arrow/tuple5_400mb_sec_f_rand.csv" // Should be some file on your system
    val spark = SparkSession.builder.appName("Euclidean Distance Spark Map").getOrCreate()
    val tupleData = spark.read
      .schema(schema)
      .csv(tupleFile)
      .cache()

    //tupleData.filter(i => i.get(0).asInstanceOf[Int] < 5).foreach(i=> println(i))

    val startTime = System.nanoTime()
    val tuplesWithDist = tupleData.mapPartitions(tuples => {


      val newTuples = tuples.map(tuple => {

        val pointX = tuple.get(0).asInstanceOf[Int]
        val pointY = tuple.get(1).asInstanceOf[Int]
        val dist = Math.sqrt(Math.pow(pointX - pointY, 2))

        Row.fromSeq(tuple.toSeq ++ Seq(dist))


      })

      newTuples


    })(encoder).cache()

    val counts = tuplesWithDist.count()


    println(counts)
    val endTime = System.nanoTime()

    println("Elapsed: " + ((endTime - startTime) / 1000000) + " ms")
    tuplesWithDist.take(5).foreach(println)

    tuplesWithDist.write.mode("overwrite").csv("/home/harry/Desktop/res/b")

    spark.stop()
  }
}
