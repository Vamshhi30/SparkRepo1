package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkComplexJson2 {

	def main(args:Array[String]):Unit ={

			val Conf = new SparkConf().setAppName("Spark Complex Json2").setMaster("local[*]")
					val Sc = new SparkContext(Conf)
					Sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					val personDF = spark.read.format("json").option("multiLine","true").load("file:///C:/Data/Complex json/person.json")
					println("=========================Raw Nested Json=========================")
					personDF.show(false)
					personDF.printSchema()
					println("=========================Flattened Json=========================")
					val flattenDF = personDF.withColumn("Students",explode(col("Students")))

					val flattenDF1 = flattenDF.select(
							col("Address.Permanent_Address"),
							col("Address.Temp_Address"),
							col("Company"),
							col("DOB"),
							col("Highest-Qualification"),
							col("Name.First Name"),
							col("Name.Last Name"),
							col("Name.Surname"),
							col("PhoneNo"),
							col("Students.Gender"),
							col("Students.Name")
							)

					flattenDF1.show(false)
					flattenDF1.printSchema()
					println("=========================Reverting Back to Nested Json=========================")

					val ComplexDF = flattenDF1.select(
							struct(
									col("Permanent_Address"),
									col("Temp_Address")
									).alias("Address"),
							col("Company"),
							col("DOB"),
							col("Highest-Qualification"),
							struct(
									col("First Name"),
									col("Last Name"),
									col("Surname")
									).alias("Name"),
							col("PhoneNo"),
							struct(
									col("Gender"),
									col("Name")
									).alias("Students")
							).groupBy("Address","Company","DOB","Highest-Qualification","Name","PhoneNo").agg(collect_list("Students").alias("Students"))
					ComplexDF.show(false)
					ComplexDF.printSchema()
	}

}