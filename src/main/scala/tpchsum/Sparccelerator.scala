/* SimpleApp.scala */
package tpchsum

import ch.jodersky.jni.nativeLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

import java.nio.ByteBuffer
import java.nio.ByteOrder
object Sparccelerator {

  @nativeLoader("sparccelerator1")
  object NativeMethod {
    @native def tpchsumtuple2(buf1: ByteBuffer, buf1Off: Int, buf1Len: Int, resBuff: ByteBuffer): Void
  }

  def main(args: Array[String]) {


    val f1 = StructField("f1", DataTypes.DoubleType)
    val f2 = StructField("f2", DataTypes.DoubleType)
    val f3 = StructField("f3", DataTypes.DoubleType)
    val f4 = StructField("f4", DataTypes.DoubleType)
    val f5 = StructField("f5", DataTypes.DoubleType)

    val fields = Array(f1, f2, f3, f4, f5)
    val schema = StructType(fields)

    //case class MyTuple5(f1: Int, f2: Int, f3: Int, f4: Int, f5: Int)


    val tupleFile = "/home/harry/Desktop/semester11/thesis/arrow/tuple5_tpch.csv" // Should be some file on your system
    val spark = SparkSession.builder.appName("TPCH sum_charge Spark JNI").getOrCreate()
    import spark.implicits._
    val tupleData = spark.read
      .schema(schema)
      .csv(tupleFile)
      .cache()

    //tupleData.filter(i => i.get(0).asInstanceOf[Int] < 5).foreach(i=> println(i))

    val startTime = System.nanoTime()

    val sum = tupleData.mapPartitions(tuples => {

      val batchSize = 100000

      var cnt = 0
      val bb = ByteBuffer.allocateDirect((8 * 5) * batchSize).order(ByteOrder.nativeOrder())
      val resBuffer = ByteBuffer.allocateDirect(3*8).order(ByteOrder.nativeOrder())
      val results = new ListBuffer[Tuple3[Double, Double, Double]]
      tuples.foreach(tuple => {

        bb.putDouble(tuple.get(0).asInstanceOf[Double])
        bb.putDouble(tuple.get(1).asInstanceOf[Double])
        bb.putDouble(tuple.get(2).asInstanceOf[Double])
        bb.putDouble(tuple.get(3).asInstanceOf[Double])
        bb.putDouble(tuple.get(4).asInstanceOf[Double])

        cnt += 1
        if (cnt == batchSize) {
          cnt = 0
          NativeMethod.tpchsumtuple2(bb, 0, batchSize, resBuffer)

          val res1 = resBuffer.getDouble()
          val res2 = resBuffer.getDouble()
          val res3 = resBuffer.getDouble()

          results += Tuple3(res1, res2, res3)
          resBuffer.flip
          bb.flip()
        }

      })

      if (cnt < batchSize && cnt != 0) {
        NativeMethod.tpchsumtuple2(bb, 0, cnt, resBuffer)

        val res1 = resBuffer.getDouble()
        val res2 = resBuffer.getDouble()
        val res3 = resBuffer.getDouble()

        results += Tuple3(res1, res2, res3)
        resBuffer.flip
      }

      Iterator(results.reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)))

    }).cache()

    val counts = sum.count()

    val finalSum = sum.reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))

    println(finalSum)
    val endTime = System.nanoTime()

    println("Elapsed: " + ((endTime - startTime) / 1000000) + " ms")
    spark.stop()
  }
}
