package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkComplexJson2 {

	def main(args:Array[String]):Unit = {

			val Conf = new SparkConf().setAppName("Spark Complex Json").setMaster("local[*]")
					val Sc = new SparkContext(Conf)
					Sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					val jsonDF = spark.read.format("json").option("multiLine",true).load("file:///C:/Data/complexjson/json_struct_multiline.json")
					println("==================Raw Nested Json===================")
					jsonDF.show(false)
					jsonDF.printSchema()
					println("==================flattened Json Data===================")
					val flattenDF = jsonDF.select(
							col("id"),
							col("image.height").alias("img_height"),
							col("image.url").alias("img_url"),
							col("image.width").alias("img_width"),
							col("name"),
							col("thumbnail.height").alias("thumbnail_height"),
							col("thumbnail.url").alias("thumbnail_url"),
							col("thumbnail.width").alias("thumbnail_width"),
							col("type")
							)

					flattenDF.show(false)
					flattenDF.printSchema()

					println("==================Reverting back to complex Json===================")

					val ComplexDF = flattenDF.select(
							col("id"),
							struct(
									col("img_height").alias("height"),
									col("img_url").alias("url"),
									col("img_width").alias("width")
									).alias("image"),
							col("name"),
							struct(
									col("thumbnail_height").alias("height"),
									col("thumbnail_url").alias("url"),
									col("thumbnail_width").alias("width")
									).alias("thumbnail"),
							col("type")
							)
					ComplexDF.show(false)
					ComplexDF.printSchema()

	}
}