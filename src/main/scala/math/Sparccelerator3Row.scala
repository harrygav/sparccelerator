/* SimpleApp.scala */
package math

import ch.jodersky.jni.nativeLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

import java.nio.ByteBuffer
import java.nio.ByteOrder


object Sparccelerator3Row {

  @nativeLoader("sparccelerator1")
  object NativeMethod {
    @native def minmax3row(buf1Off: Int, buf1Len: Int, buf1: ByteBuffer, resBuf: ByteBuffer): Void
  }

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
    val spark = SparkSession.builder.appName("Sparccelerator minmax 3 cols row format").getOrCreate()

    import spark.implicits._
    val tupleData = spark.read
      .schema(schema)
      .csv(tupleFile)
      .cache()

    //tupleData.filter(i => i.get(0).asInstanceOf[Int] < 5).foreach(i=> println(i))

    val startTime = System.nanoTime()

    val minmaxs = tupleData.mapPartitions(tuples => {

      val batchSize = 10000
      val tupleSize = 3

      var cnt = 0

      val bb1 = ByteBuffer.allocateDirect(8 * tupleSize* batchSize).order(ByteOrder.nativeOrder())

      val resBuffer = ByteBuffer.allocateDirect(6 * 8).order(ByteOrder.nativeOrder())

      val results = new ListBuffer[Tuple6[Double, Double, Double, Double, Double, Double]]
      tuples.foreach(tuple => {

        bb1.putDouble(tuple.get(0).asInstanceOf[Double])
        bb1.putDouble(tuple.get(1).asInstanceOf[Double])
        bb1.putDouble(tuple.get(2).asInstanceOf[Double])

        cnt += 1
        if (cnt == batchSize) {
          cnt = 0
          NativeMethod.minmax3row(0, batchSize, bb1, resBuffer)

          resBuffer.clear()

          val v1min = resBuffer.getDouble()
          val v1max = resBuffer.getDouble()
          val v2min = resBuffer.getDouble()
          val v2max = resBuffer.getDouble()
          val v3min = resBuffer.getDouble()
          val v3max = resBuffer.getDouble()
          results += Tuple6(v1min, v1max, v2min, v2max, v3min, v3max)

          bb1.flip()

        }

      })

      if (cnt < batchSize && cnt != 0) {

        NativeMethod.minmax3row(0, batchSize, bb1, resBuffer)

        resBuffer.clear()

        val v1min = resBuffer.getDouble()
        val v1max = resBuffer.getDouble()
        val v2min = resBuffer.getDouble()
        val v2max = resBuffer.getDouble()
        val v3min = resBuffer.getDouble()
        val v3max = resBuffer.getDouble()
        results += Tuple6(v1min, v1max, v2min, v2max, v3min, v3max)
      }

      val res = (
        results.minBy(_._1)._1, results.maxBy(_._2)._2,
        results.minBy(_._3)._3, results.maxBy(_._4)._4,
        results.minBy(_._5)._5, results.maxBy(_._6)._6
      )
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
