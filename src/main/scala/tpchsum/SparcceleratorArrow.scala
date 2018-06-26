/* SimpleApp.scala */
package tpchsum

import ch.jodersky.jni.nativeLoader
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl
import org.apache.arrow.vector.complex.writer.BaseWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

import java.nio.ByteBuffer
import java.nio.ByteOrder


object SparcceleratorArrow {

  @nativeLoader("tpchsumcol1")
  object NativeMethod {
    @native def tpchsumcol(buf1Off: Int, buf1Len: Int, buf1: ByteBuffer, buf2: ByteBuffer, buf3: ByteBuffer, buf4: ByteBuffer,
      buf5: ByteBuffer): Double
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

      val batchSize = 10000

      var cnt = 0

      /*val allocator = new RootAllocator(Integer.MAX_VALUE).asInstanceOf[BufferAllocator]

      val parent = NonNullableStructVector.empty("parent", allocator)

      val singleStructWriter = new SingleStructWriter(parent)*/


      val allocator = new RootAllocator(Integer.MAX_VALUE).asInstanceOf[BufferAllocator]

      val vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE)
      val parent = NullableMapVector.empty("parent", vectorAllocator)


      val writer = new ComplexWriterImpl("root", parent).asInstanceOf[BaseWriter.ComplexWriter]
      val rootWriter = writer.rootAsMap().asInstanceOf[BaseWriter.MapWriter]


      val double1Writer = rootWriter.float8("field_1")
      val double2Writer = rootWriter.float8("field_2")
      val double3Writer = rootWriter.float8("field_3")
      val double4Writer = rootWriter.float8("field_4")
      val double5Writer = rootWriter.float8("field_5")

      val results = new ListBuffer[Double]
      tuples.foreach(tuple => {

        double1Writer.setPosition(cnt)
        double1Writer.writeFloat8(tuple.get(0).asInstanceOf[Double])
        double2Writer.setPosition(cnt)
        double2Writer.writeFloat8(tuple.get(1).asInstanceOf[Double])
        double3Writer.setPosition(cnt)
        double3Writer.writeFloat8(tuple.get(2).asInstanceOf[Double])
        double4Writer.setPosition(cnt)
        double4Writer.writeFloat8(tuple.get(3).asInstanceOf[Double])
        double5Writer.setPosition(cnt)
        double5Writer.writeFloat8(tuple.get(4).asInstanceOf[Double])

        cnt += 1
        if (cnt == batchSize) {

          val arrowBuf = parent.getPrimitiveVectors.get(0).getDataBuffer
          val arrowBuf2 = parent.getPrimitiveVectors.get(1).getDataBuffer
          val arrowBuf3 = parent.getPrimitiveVectors.get(2).getDataBuffer
          val arrowBuf4 = parent.getPrimitiveVectors.get(3).getDataBuffer
          val arrowBuf5 = parent.getPrimitiveVectors.get(4).getDataBuffer
          val arrowBuf6 = parent.getPrimitiveVectors.get(5).getDataBuffer

          val arrowByteBuf = arrowBuf.nioBuffer.order(ByteOrder.nativeOrder)
          val arrowByteBuf2 = arrowBuf2.nioBuffer.order(ByteOrder.nativeOrder)
          val arrowByteBuf3 = arrowBuf3.nioBuffer.order(ByteOrder.nativeOrder)
          val arrowByteBuf4 = arrowBuf4.nioBuffer.order(ByteOrder.nativeOrder)
          val arrowByteBuf5 = arrowBuf5.nioBuffer.order(ByteOrder.nativeOrder)
          val arrowByteBuf6 = arrowBuf6.nioBuffer.order(ByteOrder.nativeOrder)

          cnt = 0
          results += NativeMethod.tpchsumcol(0, batchSize, arrowByteBuf, arrowByteBuf2, arrowByteBuf3, arrowByteBuf4,
                                             arrowByteBuf5)
        }

      })

      if (cnt < batchSize && cnt != 0) {

        val arrowBuf = parent.getPrimitiveVectors.get(0).getDataBuffer
        val arrowBuf2 = parent.getPrimitiveVectors.get(1).getDataBuffer
        val arrowBuf3 = parent.getPrimitiveVectors.get(2).getDataBuffer
        val arrowBuf4 = parent.getPrimitiveVectors.get(3).getDataBuffer
        val arrowBuf5 = parent.getPrimitiveVectors.get(4).getDataBuffer
        val arrowBuf6 = parent.getPrimitiveVectors.get(5).getDataBuffer

        val arrowByteBuf = arrowBuf.nioBuffer.order(ByteOrder.nativeOrder)
        val arrowByteBuf2 = arrowBuf2.nioBuffer.order(ByteOrder.nativeOrder)
        val arrowByteBuf3 = arrowBuf3.nioBuffer.order(ByteOrder.nativeOrder)
        val arrowByteBuf4 = arrowBuf4.nioBuffer.order(ByteOrder.nativeOrder)
        val arrowByteBuf5 = arrowBuf5.nioBuffer.order(ByteOrder.nativeOrder)
        val arrowByteBuf6 = arrowBuf6.nioBuffer.order(ByteOrder.nativeOrder)

        results += NativeMethod.tpchsumcol(0, cnt, arrowByteBuf, arrowByteBuf2, arrowByteBuf3, arrowByteBuf4,
                                           arrowByteBuf5)
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
