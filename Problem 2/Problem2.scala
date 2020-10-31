import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructField, StructType}

val sc = spark.sparkContext

// CHANGE PATHS HERE
val path_transactions = "./p3/data/Transactions.txt"
val path_customers = "./p3/data/Customers.txt"

// Define schemas and df
val s_t = new StructType().add("transid",StringType,true).add("custid",StringType,true).add("total",LongType,true).add("numitems",IntegerType,true).add("desc",StringType,true)
val s_c = new StructType().add("id",StringType,true).add("name",StringType,true).add("age",IntegerType,true).add("gender",StringType,true).add("country",StringType,true).add("salary",LongType,true)
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