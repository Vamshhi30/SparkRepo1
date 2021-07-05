package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkComplexJson {

	def main(args:Array[String]):Unit ={

			val Conf = new SparkConf().setAppName("Spark Complex Json").setMaster("local[*]")
					val Sc = new SparkContext(Conf)
					val spark = SparkSession.builder().getOrCreate()
					Sc.setLogLevel("Error")
					val personDF = spark.read.format("json").option("multiLine","true").load("file:///C:/Data/Complex json/person.json")
					println("============Raw Json DF==============")
					personDF.show(false)
					personDF.printSchema()
					println("==============Flattened Json DF==================")
					val flattenDF = personDF.select(
							col("Address.Permanent_Address"),
							col("Address.Temp_Address"),
							col("Company"),
							col("DOB"),
							col("Highest-Qualification"),
							col("Name.First Name"),
							col("Name.Last Name"),
							col("Name.Surname"),
							col("PhoneNo"),
							col("Students")
							).withColumn("Students",explode(col("Students")))
					flattenDF.show(false)
					flattenDF.printSchema()
					println("==============Reverting back to Nested Json==================")
					val ComplexDF = flattenDF.select(
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
							col("Students")
							)

					val ComplexDF1 = ComplexDF.groupBy("Address","Company","DOB","Highest-Qualification","Name","PhoneNo").agg(collect_list("Students").alias("Students"))
					ComplexDF1.show(false)
					ComplexDF1.printSchema()



	}

}