package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source

object SparkFlattenRandomUserJson {
	def main(args:Array[String]):Unit = {
			val Conf = new SparkConf().setAppName("Spark Random User Json").setMaster("local[*]")
					val Sc = new SparkContext(Conf)
					val spark = SparkSession.builder().getOrCreate()
					Sc.setLogLevel("Error")
					val urlData = Source.fromURL("https://randomuser.me/api/0.8/?results=100").mkString
					val randomUserRdd = Sc.parallelize(List(urlData))
					val randomUserDF = spark.read.json(randomUserRdd)
					println("==================Raw Nested Json Data====================")
					randomUserDF.show(false)
					randomUserDF.printSchema()

					println("==================Flattened Json Data====================")
					val flattenDF = randomUserDF.withColumn("results",explode(col("results")))

					flattenDF.printSchema()

					val flattenDF1 = flattenDF.select(
							col("nationality"),
							col("results.user.*"),
							col("seed"),
							col("version")
							)
					val flattenDF2 = flattenDF1.select(
							"nationality","cell","dob","email","gender","location.*","md5","name.*","password","phone","picture.*","registered","salt","sha1","sha256","username","seed","version"
							)
					flattenDF2.show(false)
					flattenDF2.printSchema()


	}

}