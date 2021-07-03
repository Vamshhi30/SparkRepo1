package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkComplexJsonData {

	def main(args:Array[String]):Unit ={
			val Conf = new SparkConf().setAppName("Spark Complex Json").setMaster("local[*]")
					val sc = new SparkContext(Conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().appName("Spark Complex Json").master("local[*]").getOrCreate()
					val json_df = spark.read.format("json").option("multiLine","true").load("file:///C:/Data/Complex json/person.json")
					println("=======================raw Nested Json Data=============================")
					json_df.show(false) 
					json_df.printSchema()
					println("=======================Flattened Json Data=============================")
					val flatten_df = json_df.select(
							//col("Address.*"),
							col("Address.Permanent_Address"),
							col("Address.Temp_Address"),
							col("Company"),
							col("DOB"),
							col("Highest-Qualification"),
							col("Name.First Name"),
							col("Name.Last Name"),
							col("Name.Surname"),
							col("PhoneNo")
							)
					flatten_df.show(false)
					flatten_df.printSchema()
					println("=======================converting back flattened json to complex Json Data=============================")
					val complex_json = flatten_df.select(
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
							col("PhoneNo")		
							)
					complex_json.show(false)
					complex_json.printSchema()
	}
}