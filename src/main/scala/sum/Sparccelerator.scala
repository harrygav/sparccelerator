/* SimpleApp.scala */
package sum

import ch.jodersky.jni.nativeLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

import java.nio.ByteBuffer
import java.nio.ByteOrder


object Sparccelerator {

  @nativeLoader("sum1")
  object NativeMethod {
    @native def testC(buf1: ByteBuffer, buf1Off: Int, buf1Len: Int): Int
  }

  def main(args: Array[String]) {


    val f1 = StructField("f1", DataTypes.IntegerType)
    val f2 = StructField("f2", DataTypes.IntegerType)
    val f3 = StructField("f3", DataTypes.IntegerType)
    val f4 = StructField("f4", DataTypes.IntegerType)
    val f5 = StructField("f5", DataTypes.IntegerType)

    val fields = Array(f1, f2, f3, f4, f5)
    val schema = StructType(fields)


    //case class MyTuple5(f1: Int, f2: Int, f3: Int, f4: Int, f5: Int)


    val tupleFile = "/home/harry/Desktop/semester11/thesis/arrow/tuple5.csv" // Should be some file on your system
    val spark = SparkSession.builder.appName("Sum 2 Fields Spark JNI").getOrCreate()
    import spark.implicits._
    val tupleData = spark.read
      .schema(schema)
      .csv(tupleFile)
      .cache()

    //tupleData.filter(i => i.get(0).asInstanceOf[Int] < 5).foreach(i=> println(i))

    val startTime = System.nanoTime()

    val sum = tupleData.mapPartitions(tuples => {

      val batchSize = 10000

      var cnt = 0
      val bb = ByteBuffer.allocateDirect((4 * 5) * batchSize).order(ByteOrder.nativeOrder())
      val results = new ListBuffer[Int]
      tuples.foreach(tuple => {

        bb.putInt(tuple.get(0).asInstanceOf[Int])
        bb.putInt(tuple.get(1).asInstanceOf[Int])
        bb.putInt(tuple.get(2).asInstanceOf[Int])
        bb.putInt(tuple.get(3).asInstanceOf[Int])
        bb.putInt(tuple.get(4).asInstanceOf[Int])

        cnt += 1
        if (cnt == batchSize) {
          cnt = 0
          results += NativeMethod.testC(bb, 0, batchSize)
          bb.flip()
        }

      })

      if (cnt < batchSize && cnt!=0) {
        results += NativeMethod.testC(bb, 0, cnt)
      }


      Iterator(results.reduce((a, b) => a + b))

    }).cache()

    val counts = sum.count()

    val finalSum = sum.reduce((a, b) => a + b)

    println(finalSum)
    val endTime = System.nanoTime()

    println("Elapsed: " + ((endTime - startTime) / 1000000) + " ms")
    spark.stop()
  }
}
