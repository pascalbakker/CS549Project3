name := "project3"
version := "1.0"
scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
	"org.scalatest" %% "scalatest" % "3.2.2" % "test",
	"org.apache.spark" %% "spark-core" % "3.0.1",
	"org.apache.spark" %% "spark-sql" % "3.0.1"
)
