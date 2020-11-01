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
val j2 = j1.withColumn("Space", when($"Cell" === 1 or $"Cell" === 500 or $"Cell" === 249501 or $"Cell" === 250000, 3).otherwise(when($"Cell"%500 === 0 or ($"Cell"-1)%500 === 0 or $"Cell" < 501 or $"Cell" > 249500, 5).otherwise(8))).select("Cell","n","neighbors", "Space")
 
// Calculate index and print result in console
val j3 = j2.withColumn("I", (col("n")*col("Space"))/col("neighbors")).sort(col("I").desc)
val j4 = j3.select("Cell","I").limit(50)
j4.repartition(1).write.option("header","true").csv("data/results/Top50Index.csv")


// Top 50 grid cells and their neighbors
val s = r.withColumnRenamed("Cell", "Top 50 Cell").withColumnRenamed("C1", "Neighbor Cell").select("Top 50 Cell", "Neighbor Cell")
val s1 = s.join(j3.select("Cell", "I"), s("Neighbor Cell") === j3("Cell")).withColumnRenamed("I", "I Neighbor Cell").select("Top 50 Cell", "Neighbor Cell", "I Neighbor Cell")
val s2 = j4.select("Cell").join(s1, j4("Cell") === s1("Top 50 Cell")).select("Top 50 Cell", "Neighbor Cell", "I Neighbor Cell")
s2.repartition(1).write.option("header","true").csv("data/results/Top50NeighborsIndex.csv")

