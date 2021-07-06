package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkComplexJson6 {

	def main(args:Array[String]):Unit ={
			val Conf = new SparkConf().setAppName("Spark Complex Json").setMaster("local[*]")
					val sc = new SparkContext(Conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					val complexjsonDF = spark.read.format("json").option("multiLine","true").load("file:///C:/Data/Complex json/person.json")

					println("============Raw Nested Json=============")
					complexjsonDF.show(false)
					complexjsonDF.printSchema()
					println("============Flattened Json=============")
					val flattenDF = complexjsonDF.select(
							col("Address.*"),
							col("Company"),
							col("DOB"),
							col("Highest-Qualification"),
							col("Name.*"),
							col("PhoneNo"),
							col("Students")).withColumn("Students",explode(col("Students")))

					val flattenDF1 = flattenDF.select(
							col("Permanent_Address"),
							col("Temp_Address"),
							col("Company"),
							col("DOB"),
							col("Highest-Qualification"),
							col("First Name"),
							col("Last Name"),
							col("Surname"),
							col("PhoneNo"),
							col("Students.Gender"),
							col("Students.Name")
							)

					flattenDF1.show(false)
					flattenDF1.printSchema()

					println("============Flattened to Complex Json=============")
					val ComplexJson = flattenDF1.select(
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

					ComplexJson.show(false)
					ComplexJson.printSchema()
	}
}