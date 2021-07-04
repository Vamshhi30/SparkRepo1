package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkDSL_Task2 
{

	def main(args:Array[String]):Unit ={

			val Conf = new SparkConf().setAppName("Spark DSL Task2").setMaster("local[*]")
					val Sc = new SparkContext(Conf)
					Sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					val reqapi_DF = spark.read.format("json").option("multiLine","true").load("file:///C:/Data/Complex json/reqapi.json")
					//Raw reqapi Json data
					println("========================Raw reqapi Nested Json data=============================")
					reqapi_DF.show(false)
					reqapi_DF.printSchema()

					println("========================Flattened reqapi Json data=============================")
					val flattened_DF = reqapi_DF.select(
							//col("data.*"),
							col("data.avatar"),
							col("data.email"),
							col("data.first_name"),
							col("data.id"),
							col("data.last_name"),
							col("page"),
							col("per_page"),
							//col("support.*"),
							col("support.text"),
							col("support.url"),
							col("total"),
							col("total_pages")
							)
					flattened_DF.show(false)
					flattened_DF.printSchema()

					println("=================Converting Flattened Json DF to Nested Json==================")
					val complex_DF = flattened_DF.select(
							struct(
									col("avatar"),
									col("email"),
									col("first_name"),
									col("id"),
									col("last_name")
									).alias("data"),
							col("page"),
							col("per_page"),
							struct(
									col("text"),
									col("url")
									).alias("support"),
							col("total"),
							col("total_pages")
							)
					complex_DF.show(false)
					complex_DF.printSchema()
	}
}