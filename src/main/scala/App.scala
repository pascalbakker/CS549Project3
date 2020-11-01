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
      deleteFolder("data/results/t1.csv")
      deleteFolder("data/results/t6.csv")
      //Problem1(spark)
      if(args.length == 2 && ags(1)=="True"){
        val matrix1 = MatrixGenerator(2,2,"data/matrix1.csv")
        val matrix2 = MatrixGenerator(2,2,"data/matrix2.csv")
      }
      deleteFolder("data/results/matrix3.csv")
      //Problem4(spark,"data/matrix1.csv", "data/matrix2.csv")
      val result = args(0) match {
          case "1" => Problem1(spark)
          case "2" => Problem2(spark)
          case "3" => Problem3(spark)
          case "4" => Problem4(spark, "data/matrix1.csv","data/matrix2.csv")
      }
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

  def Problem2(spark: SparkSession): Unit = {
      import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructField, StructType}

      val sc = spark.sparkContext

      // CHANGE PATHS HERE
      val path_transactions = "data/Transactions.txt"
      val path_customers = "data/Customers.txt"

      // Define schemas and df
      val s_t = new StructType()
                    .add("transid",StringType,true)
                    .add("custid",StringType,true)
                    .add("total",LongType,true)
                    .add("numitems",IntegerType,true)
                    .add("desc",StringType,true)
      val s_c = new StructType()
                    .add("id",StringType,true)
                    .add("name",StringType,true)
                    .add("age",IntegerType,true)
                    .add("gender",StringType,true)
                    .add("country",StringType,true)
                    .add("salary",LongType,true)
      val t = spark.read.schema(s_t).csv(path_transactions)
      val c = spark.read.schema(s_c).csv(path_customers)

      // Aggregate transactions
      val t1 = t.groupBy("custid").agg(expr("sum(total) as total"))
      // Select customer fields
      val c1 = c.select("id", "gender", "country")

      // Get largest sum by country
      val j1 = t1.join(c1, t1("custid") === c1("id"))
      val j2 = j1.groupBy("country").agg(expr("sum(total) as total"))
      val k = j2.groupBy().max("total").collect()(0)(0).toString.toInt
      val j3 = j2.where(j2("total") === k).select(expr("country as cou"))

      // Elaborate table for result
      val r1 = j1.join(j3, j1("country") === j3("cou"))
      val r2 = r1.groupBy("country", "gender").agg(count("gender"))

      // Print output
      r2.show()
      r2.repartition(1).write.option("header","true").csv("data/results/r2.csv")
  }

  def Problem3(spark: SparkSession): Unit = {
    import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructField, StructType}
    val sc = spark.sparkContext

    // CHANGE PATH HERE
    val path_data = "./points.txt"

    // Create df and get p3 as the points and their correspondent cell
    val s_p = new StructType().add("x",IntegerType,true).add("y",IntegerType,true)
    val p = spark.read.schema(s_p).csv(path_data)
    val p1 = p.withColumn("Rx", (col("x") - 1 - ((col("x")-1)%20))/20)
    val p2 = p1.withColumn("Ry", (col("y") - 1 - ((col("y")-1)%20))/20)
    val p3 = p2.withColumn("Cell", col("Rx") + col("Ry")*500 + 1)
    // Aggregate points per cell
    val c = p3.groupBy("Cell").agg(expr("count(Cell) as n"))

    // Assign each cell to its potential neighbors
    val c1 = c.where("Cell%500 > 0").withColumn("C1", col("Cell") + 1)
    val c2 = c.where("(Cell-1)%500 > 0").withColumn("C1", col("Cell") - 1)

    val c4 = c.where("Cell < 249501").withColumn("C1", col("Cell") + 500)
    val c7 = c.where("Cell > 500").withColumn("C1", col("Cell") - 500)

    val c3 = c.where("Cell < 249501 and (Cell-1)%500 > 0").withColumn("C1", col("Cell") + 499)
    val c5 = c.where("Cell < 249501 and Cell%500 > 0").withColumn("C1", col("Cell") + 501)

    val c6 = c.where("Cell > 500 and Cell%500 > 0").withColumn("C1", col("Cell") - 499)
    val c8 = c.where("Cell > 500 and (Cell-1)%500 > 0").withColumn("C1", col("Cell") - 501)

    // Merge de data of the potential neighbors and aggregate
    val r = c1.union(c2).union(c3).union(c4).union(c5).union(c6).union(c7).union(c8)
    val r1 = r.groupBy("C1").agg(expr("sum(n) as neighbors"))

    // Join points per cell with data of neighbors
    val j1 = c.join(r1, c("Cell") === r1("C1"))
    // Calculate the number of cells next to each cell to get the average
    val j2 = j1.withColumn("Space", when(col("Cell") === 1 or col("Cell") === 500 or col("Cell") === 249501 or col("Cell") === 250000, 3).otherwise(when(col("Cell")%500 === 0 or (col("Cell")-1)%500 === 0 or col("Cell") < 501 or col("Cell") > 249500, 5).otherwise(8))).select("Cell","n","neighbors", "Space")

    // Calculate index and print result in console
    val j3 = j2.withColumn("I", (col("n")*col("Space"))/col("neighbors")).sort(col("I").desc)
    val j4 = j3.select("Cell","I").limit(50)
    j4.repartition(1).write.option("header","true").csv("data/results/Top50Index.csv")


    // Top 50 grid cells and their neighbors
    val s = r.withColumnRenamed("Cell", "Top 50 Cell").withColumnRenamed("C1", "Neighbor Cell").select("Top 50 Cell", "Neighbor Cell")
    val s1 = s.join(j3.select("Cell", "I"), s("Neighbor Cell") === j3("Cell")).withColumnRenamed("I", "I Neighbor Cell").select("Top 50 Cell", "Neighbor Cell", "I Neighbor Cell")
    val s2 = j4.select("Cell").join(s1, j4("Cell") === s1("Top 50 Cell")).select("Top 50 Cell", "Neighbor Cell", "I Neighbor Cell")
    s2.repartition(1).write.option("header","true").csv("data/results/Top50NeighborsIndex.csv")

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

    val m3 = m1.join(m2, Seq("j"),"inner")
                 .withColumn("v",expr("m*n"))
                 .groupBy("i","k")
                 .agg(sum("v"))

    m3.repartition(1).write.option("header","true").csv("data/results/matrix3.csv")
    sc.stop();
  }

  def deleteFolder(path:String): Unit ={
    new File(path).delete()
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


