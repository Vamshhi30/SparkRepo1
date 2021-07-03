package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Spark_reqapi {
	def main(args:Array[String]):Unit = {

			val Conf = new SparkConf().setAppName("Spark Complex Json reqapi").setMaster("local[*]")
					val sc = new SparkContext(Conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					val data_list = List("avatar","email","first_name","id","last_name")
					val support_list = List("text","url")
					val reqapi_df = spark.read.format("json").option("multiLine","true").load("file:///C:/Data/Complex json/reqapi.json")
					println("============Raw Nested Json Data=================")
					reqapi_df.show(false)
					reqapi_df.printSchema()
					println("============Flattened Json Data==================")
					val flattened_df = reqapi_df.select(
							col("data.*"),
							//col("data.avatar"),
							//col("data.email"),
							//col("data.first_name"),
							//col("data.id"),
							//col("data.last_name"),
							col("page"),
							col("per_page"),
							col("support.*"),
							//col("support.text"),
							//col("support.url"),
							col("total"),
							col("total_pages")
							)
					flattened_df.show(false)
					flattened_df.printSchema()
					println("============Converting Flattened data to nested json==================")
					val complex_jsonDF = flattened_df.select(
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
					complex_jsonDF.show(false)
					complex_jsonDF.printSchema()
	}
}