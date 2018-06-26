/* SimpleApp.scala */
package euclidean

import ch.jodersky.jni.nativeLoader
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import sun.nio.ch.DirectBuffer

import scala.collection.mutable.ListBuffer

import java.nio.ByteBuffer
import java.nio.ByteOrder


object Sparccelerator {

  def getUnsafeInstance: sun.misc.Unsafe = {
    val f = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    f.setAccessible(true)
    val unsafe = f.get(null).asInstanceOf[sun.misc.Unsafe]
    unsafe
  }

  @nativeLoader("euclidean1")
  object NativeMethod {
    @native def euclidean(buf1: ByteBuffer, buf1Off: Int, buf1Len: Int, resbuf: ByteBuffer): Void
  }

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

    //case class MyTuple5(f1: Int, f2: Int, f3: Int, f4: Int, f5: Int)


    val tupleFile = "/home/harry/Desktop/semester11/thesis/arrow/tuple5_2gb.csv" // Should be some file on your system
    //val tupleFile = "/home/harry/Desktop/semester11/thesis/arrow/tuple5_4gb_sec_fourth_rand.csv" // Should be some file on your system
    val spark = SparkSession.builder.appName("Euclidian Distance Sparccelerator").getOrCreate()
    val tupleData = spark.read
      .schema(schema)
      .csv(tupleFile)
      .cache()

    //tupleData.filter(i => i.get(0).asInstanceOf[Int] < 5).foreach(i=> println(i))

    val startTime = System.nanoTime()

    val tuplesWithDist = tupleData.mapPartitions(tuples => {

      val batchSize = 10000

      //try unsafe
      val unsafe = getUnsafeInstance

      //initialize ByteBuffer for serialized tuples and result
      var cnt = 0
      val bb = ByteBuffer.allocateDirect((4 * 5) * batchSize).order(ByteOrder.nativeOrder())
      val resBuffer = ByteBuffer.allocateDirect(8 * batchSize).order(ByteOrder.nativeOrder())

      val bbAddr = bb.asInstanceOf[DirectBuffer].address()
      var newbbAddr = bbAddr
      val resBufAddr = resBuffer.asInstanceOf[DirectBuffer].address()
      var newResBufAddr = resBufAddr

      val results = new ListBuffer[Row]()

      //serialize batches of tuples
      tuples.foreach(tuple => {

        /*bb.putInt(tuple.get(0).asInstanceOf[Int])
        bb.putInt(tuple.get(1).asInstanceOf[Int])
        bb.putInt(tuple.get(2).asInstanceOf[Int])
        bb.putInt(tuple.get(3).asInstanceOf[Int])
        bb.putInt(tuple.get(4).asInstanceOf[Int])*/

        unsafe.putInt(newbbAddr, tuple.get(0).asInstanceOf[Int])
        unsafe.putInt(newbbAddr + 4, tuple.get(1).asInstanceOf[Int])
        unsafe.putInt(newbbAddr + 8, tuple.get(2).asInstanceOf[Int])
        unsafe.putInt(newbbAddr + 12, tuple.get(3).asInstanceOf[Int])
        unsafe.putInt(newbbAddr + 16, tuple.get(4).asInstanceOf[Int])

        //increase address by 20 bytes (5xintegers of 4 bytes) && tuple counter
        newbbAddr += 20
        cnt += 1

        //if batch is completed send to C process
        if (cnt == batchSize) {
          cnt = 0
          NativeMethod.euclidean(bb, 0, batchSize, resBuffer)

          //reset ByteBuffers markers and construct new output tuple batch with result
          //bb.clear()
          //resBuffer.clear()

          //start reading at beginning of batch data & result
          newbbAddr = bbAddr
          newResBufAddr = resBufAddr
          for (j <- 0 to batchSize - 1) {

            results += Row.fromSeq(Seq(unsafe.getInt(newbbAddr), unsafe.getInt(newbbAddr + 4), unsafe.getInt(newbbAddr + 8),
                                       unsafe.getInt(newbbAddr + 12), unsafe.getInt(newbbAddr + 16)) ++
                                     Seq(unsafe.getDouble(newResBufAddr)))

            newbbAddr += 20
            newResBufAddr += 8
          }

          //data has been processed, reset address offsets of Buffers
          newbbAddr = bbAddr
          newResBufAddr = resBufAddr
          //bb.flip()
          //resBuffer.flip()
        }

      })

      //process remaining tuples that did not form a full batch

      if (cnt < batchSize && cnt != 0) {
        NativeMethod.euclidean(bb, 0, cnt, resBuffer)

        //bb.clear()
        //resBuffer.clear()

        newbbAddr = bbAddr
        newResBufAddr = resBufAddr

        for (j <- 0 to cnt - 1) {
          results += Row.fromSeq(Seq(unsafe.getInt(newbbAddr), unsafe.getInt(newbbAddr + 4), unsafe.getInt(newbbAddr + 8),
                                     unsafe.getInt(newbbAddr + 12), unsafe.getInt(newbbAddr + 16)) ++
                                   Seq(unsafe.getDouble(newResBufAddr)))

          newbbAddr += 20
          newResBufAddr += 8
        }
      }


      results.toIterator

    })(encoder).cache()

    val counts = tuplesWithDist.count()

    println(counts)
    val endTime = System.nanoTime()

    println("Elapsed: " + ((endTime - startTime) / 1000000) + " ms")

    tuplesWithDist.take(5).foreach(println)
    tuplesWithDist.write.mode("overwrite").csv("/home/harry/Desktop/res/a")
    spark.stop()
  }
}
