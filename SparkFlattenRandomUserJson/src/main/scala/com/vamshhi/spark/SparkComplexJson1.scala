package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkComplexJson1 {

	def main(args:Array[String]):Unit ={

			val Conf = new SparkConf().setAppName("Spark Complex Json").setMaster("local[*]")
					val Sc = new SparkContext(Conf)
					Sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					val actorsDF = spark.read.format("json").option("multiLine",true).load("file:///C:/Data/complexjson/actors.json")

					println("=====================Raw Json Data=============================")
					actorsDF.show(false)
					actorsDF.printSchema()

					println("=====================Flattened Json Data=============================")
					val flattenDF = actorsDF.withColumn("Actors",explode(col("Actors"))).select(
							col("Actors.*"),
							col("country"),
							col("version")
							).withColumn("children",explode(col("children")))
					flattenDF.show(false)
					flattenDF.printSchema()
	}
}