package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkComplexJson8 {
	def main(args:Array[String]):Unit = {

			val Conf = new SparkConf().setAppName("Spark Complex Json 8").setMaster("local[*]")
					val sc = new SparkContext(Conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					val jsonDF = spark.read.format("json").option("multiLine","true").load("file:///C:/Data/complexjson/zeyo27_json.json")
					println("=====================Raw Complex Json=====================")
					jsonDF.show(false)
					jsonDF.printSchema()
					println("=====================Flattened Json=====================")
					val flattenDF = jsonDF.withColumn("students",explode(col("students"))).select(
							col("address.permanent_address"),
							col("address.temporary_address"),
							col("first_name"),
							col("second_name"),
							col("students.location"),
							col("students.name"),
							col("trainer")
							)

					flattenDF.show(false)
					flattenDF.printSchema()
					println("=====================Reverting Back to Complex Json=====================")

					val ComplexDF = flattenDF.select(
							struct(
									col("permanent_address"),
									col("temporary_address")
									).alias("address"),
							col("first_name"),
							col("second_name"),
							struct(
									col("location"),
									col("name")
									).alias("students"),
							col("trainer")
							).groupBy("address","first_name","second_name","trainer").agg(collect_list("students").alias("students"))

					val ComplexDF1 = ComplexDF.select(
							col("address"),
							col("first_name"),
							col("second_name"),
							col("students"),
							col("trainer")
							)		
					ComplexDF1.show(false)
					ComplexDF1.printSchema()
	}

}