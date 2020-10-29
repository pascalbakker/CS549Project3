package scala
// import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.SaveMode
object main extends App{
  Console.println("Testing scala");

  override def main(args: Array[String]): Unit = {
      val spark: SparkSession = SparkSession.builder()
        .master("local")
        .appName("Problem1")
        .getOrCreate()
      Problem1(spark)
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
      t1.write.option("header","true").csv("data/results/t1.csv")
      t2.write.option("header","true").csv("data/results/t2.csv")
      t3.write.option("header","true").csv("data/results/t3.csv")
      t4.write.option("header","true").csv("data/results/t4.csv")
      t5.write.option("header","true").csv("data/results/t5.csv")
      t6.write.option("header","true").csv("data/results/t6.csv")
      sc.stop()
  }
}


