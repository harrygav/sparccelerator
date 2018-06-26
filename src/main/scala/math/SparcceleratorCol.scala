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


object SparcceleratorCol {

  @nativeLoader("sparccelerator1")
  object NativeMethod {
    @native def minmax(buf1Off: Int, buf1Len: Int, buf1: ByteBuffer, resBuf: ByteBuffer): Void
  }

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
    val spark = SparkSession.builder.appName("Sparccelerator minmax").getOrCreate()

    import spark.implicits._
    val tupleData = spark.read
      .schema(schema)
      .csv(tupleFile)
      .cache()

    //tupleData.filter(i => i.get(0).asInstanceOf[Int] < 5).foreach(i=> println(i))

    val startTime = System.nanoTime()

    val minmaxs = tupleData.mapPartitions(tuples => {

      val batchSize = 1000
      val tupleSize = 5

      var cnt = 0

      val bb1 = ByteBuffer.allocateDirect(8*batchSize).order(ByteOrder.nativeOrder())

      val resBuffer = ByteBuffer.allocateDirect(2 * 8).order(ByteOrder.nativeOrder())

      val results = new ListBuffer[Tuple2[Double, Double]]
      tuples.foreach(tuple => {

        bb1.putDouble(tuple.get(0).asInstanceOf[Double])

        cnt += 1
        if (cnt == batchSize) {
          cnt = 0
          NativeMethod.minmax(0, batchSize, bb1, resBuffer)

          resBuffer.clear()

          val res1 = resBuffer.getDouble()
          val res2 = resBuffer.getDouble()
          results += Tuple2(res1, res2)

          bb1.clear()

        }

      })

      if (cnt < batchSize && cnt != 0) {

        NativeMethod.minmax(0, cnt, bb1, resBuffer)

        resBuffer.clear()

        val res1 = resBuffer.getDouble()
        val res2 = resBuffer.getDouble()
        results += Tuple2(res1, res2)
      }

      val res = (results.minBy(_._1), results.maxBy(_._2))._1
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
