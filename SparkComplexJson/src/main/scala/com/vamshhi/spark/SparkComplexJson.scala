package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source

object SparkComplexJson 
{
	def main(args:Array[String]):Unit = {

			val Conf = new SparkConf().setAppName("Spark Complex Json").setMaster("local[*]")
					val sc = new SparkContext(Conf)
					val spark = SparkSession.builder().getOrCreate()
					sc.setLogLevel("Error")
					val url_data = Source.fromURL("https://randomuser.me/api/0.8/?results=10").mkString
					val randomUserRDD = sc.parallelize(List(url_data))
					val randomUserDF = spark.read.json(randomUserRDD)

					println("======================Raw Complex Json==========================")
					//randomUserDF.show(false)
					randomUserDF.printSchema()

					println("======================Flattening Complex Json==========================")

					val flattenDF = randomUserDF.withColumn("results",explode(col("results"))).select(
							col("nationality"),
							col("results.user.cell"),
							col("results.user.dob"),
							col("results.user.email"),
							col("results.user.gender"),
							col("results.user.location.*"),
							col("results.user.md5"),
							col("results.user.name.*"),
							col("results.user.password"),
							col("results.user.phone"),
							col("results.user.picture.*"),
							col("results.user.registered"),
							col("results.user.salt"),
							col("results.user.sha1"),
							col("results.user.sha256"),
							col("results.user.username"),
							col("seed"),
							col("version")
							)

					flattenDF.show(false)
					flattenDF.printSchema()
					//flattenDF.coalesce(1).write.format("json").mode("overwrite").save("file:///C:/Data/complexjson/randomUserJson")

					println("======================Reverting Back to Complex Json==========================")
					val ComplexDF = flattenDF.select(
							col("nationality"),
							struct(
									col("cell"),
									col("dob"),
									col("email"),
									col("gender"),
									struct(
											col("city"),
											col("state"),
											col("street"),
											col("zip")
											).alias("location"),
									col("md5"),
									struct(
											col("first"),
											col("last"),
											col("title")
											).alias("name"),
									col("password"),
									col("phone"),
									struct(
											col("large"),
											col("medium"),
											col("thumbnail")
											).alias("picture"),
									col("registered"),
									col("salt"),
									col("sha1"),
									col("sha256"),
									col("username")).alias("user"),
							col("seed"),
							col("version")
							).groupBy("nationality","seed","version").agg(collect_list(struct("user").alias("user")).alias("results"))

					val ComplexDF1 = ComplexDF.select(
							col("nationality"),
							col("results"),
							col("seed"),
							col("version")
							)
					//ComplexDF1.show(false)
					ComplexDF1.printSchema()
	}
}