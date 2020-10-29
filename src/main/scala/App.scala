package scala
// import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.SaveMode
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.File
object main extends App{
  Console.println("Testing scala");

  override def main(args: Array[String]): Unit = {
      val spark: SparkSession = SparkSession.builder()
        .master("local")
        .appName("Problem1")
        .getOrCreate()
      //Problem1(spark)
      val matrix1 = MatrixGenerator(500,500,"data/matrix1.csv")
      val matrix2 = MatrixGenerator(500,500,"data/matrix2.csv")
      Problem4(spark,"data/matrix1.csv", "data/matrix2.csv")

  }
  // Transcations: transid, custid,total,numitems
  def Problem1(spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    val schema = new StructType()
                      .add("transid",IntegerType,true)
                      .add("custid",IntegerType,true)
                      .add("total",IntegerType,true)
                      .add("numitems",IntegerType,true)
                      .add("desc",StringType,true)
    val t = spark.read.schema(schema).csv("data/Transactions.csv")

    val t1 = t.where(t("total") < 200)
    val t2 = t1.groupBy("numitems")
      .agg(
        sum("total"),
        avg("total"),
        min("total"),
        max("total")
      )

      val t3 = t1.groupBy("custid")
        .agg(
          count("custid")
        ).withColumnRenamed("count(custid)", "t3count")

      val t4 = t.where(t("total") < 600)
      val t5 = t4.groupBy("custid")
        .agg(
          count("custid")
        ).withColumnRenamed("count(custid)", "t5count")

      val t6 = t5.join(t3, Seq("custid"),"inner").filter("t3count<t5count*5")
      //val t6 = t5.filter(t5("count(custid)")*5<=t3("count(custid)"))
      t1.repartition(1).write.option("header","true").csv("data/results/t1.csv")
      t6.repartition(1).write.option("header","true").csv("data/results/t6.csv")
      sc.stop()
  }


  // Each matrix is a csv
  // i,j, value
  def Problem4(spark: SparkSession, m1path: String, m2path: String ): Unit = {
    val sc = spark.sparkContext
    val schema = new StructType()
                      .add("i",IntegerType,true)
                      .add("j",IntegerType,true)
                      .add("n",IntegerType,true)
    val schema2 = new StructType()
                      .add("j",IntegerType,true)
                      .add("k",IntegerType,true)
                      .add("m",IntegerType,true)
    val m1 = spark.read.schema(schema).csv("data/matrix1.csv")
    val m2 = spark.read.schema(schema2).csv("data/matrix2.csv")

    val join = m1.join(m2, Seq("j"),"inner")
    val reduce = join.withColumn("v",expr("m*n"))
    val groupBy = reduce.groupBy("i","k").agg(
      sum("v")
    )

    groupBy.repartition(1).write.option("header","true").csv("data/results/matrix3.csv")
    sc.stop();
  }


  def writeMatrix(matrix: Array[Array[Integer]], filepath: String): Unit = {
    val file = new java.io.File(filepath)
    val bw = new BufferedWriter(new FileWriter(file))
    for(i <- 0 to (matrix.length-1); j <- 0 to (matrix(i).length-1)){
      val line = i + ","+j+","+matrix(i)(j)+"\n"
      bw.write(line)
    }
    bw.close()
  }

  def MatrixGenerator(height: Integer, width: Integer, filepath: String): Array[Array[Integer]] = {
      var matrix = Array.ofDim[Integer](width,height)
      val r = scala.util.Random
      for(i <- 0 to width-1; j <- 0 to height-1){
          matrix(i)(j) = r.nextInt(100)
      }
      writeMatrix(matrix,filepath)
      matrix
  }
}


