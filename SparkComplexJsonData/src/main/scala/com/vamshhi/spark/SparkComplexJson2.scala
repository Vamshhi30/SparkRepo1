package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkComplexJson2 {
	def main(args:Array[String]):Unit ={
			val Conf = new SparkConf().setAppName("Complex Json2").setMaster("local[*]")
					val sc = new SparkContext(Conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					val jsonDF1 = spark.read.format("json").option("multiLine","true").load("file:///C:/Data/Complex json/nested_json.json")
					println("=================Raw Nested Json Data==================")
					jsonDF1.show(false)
					jsonDF1.printSchema()
					println("=================Flattened Json Data==================")
					val flattened_DF = jsonDF1.select(
							col("boolean_key"),
							col("empty_string_translation"),
							col("key_with_description"),
							col("key_with_line-break"),
							col("nested.deeply.key").alias("nested_deeply_key"),
							col("nested.key").alias("nested_key"),
							col("null_translation"),
							col("pluralized_key.one"),
							col("pluralized_key.other"),
							col("pluralized_key.zero"),
							col("sample_collection.first").alias("first_item"),
							col("sample_collection.second").alias("second_item"),
							col("sample_collection.third").alias("third_item"),
							col("simple_key"),
							col("unverified_key")
							)
					flattened_DF.show(false)
					flattened_DF.printSchema()
					println("=================Converting Flattened Json to Complex Nested Json Data==================")
					val complex_DF = flattened_DF.select(
							col("boolean_key"),
							col("empty_string_translation"),
							col("key_with_description"),
							col("key_with_line-break"),
							struct(
									struct(
											col("nested_deeply_key").alias("key")
											).alias("deeply"),
									col("nested_key").alias("key")
									).alias("nested"),
							col("null_translation"),
							struct(
									col("one"),
									col("other"),
									col("zero")
									).alias("pluralized_key"),
							struct(
									col("first_item").alias("first"),
									col("second_item").alias("second"),
									col("third_item").alias("third")
									).alias("sample_collection"),
							col("simple_key"),
							col("unverified_key")
							)
					complex_DF.show(false)
					complex_DF.printSchema()

	}

}