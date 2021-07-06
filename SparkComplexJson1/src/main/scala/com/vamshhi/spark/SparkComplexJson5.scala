package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkComplexJson5 {

	def main(args:Array[String]):Unit ={
			val Conf = new SparkConf().setAppName("Spark Complex Json").setMaster("local[*]")
					val sc = new SparkContext(Conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					val complexjsonDF = spark.read.format("json").option("multiLine","true").load("file:///C:/Data/complexjson/Arrays_structs.json")
					println("==============Complex Nested Json===============")
					complexjsonDF.show(false)
					complexjsonDF.printSchema()

					val flattenDF = complexjsonDF.select(
							col("batters.batter"),
							col("id"),
							col("name"),
							col("ppu"),
							col("topping"),
							col("type")
							).withColumn("batter",explode(col("batter")))
					.withColumn("topping",explode(col("topping")))


					val final_DF = flattenDF.select(
							col("batter.id").alias("batter_id"),
							col("batter.type").alias("batter_type"),
							col("id"),
							col("name"),
							col("ppu"),
							col("topping.id").alias("topping_id"),
							col("topping.type").alias("topping_type"),
							col("type")
							)

					println("==============Flattened Json===============")
					//final_DF.show(false)		
					final_DF.printSchema()

					println("============Reverting back to nested Json============")

					val ComplexDF = final_DF.select(
							struct(
									col("batter_id").alias("id"),
									col("batter_type").alias("type")
									).alias("batter"),
							col("id"),
							col("name"),
							col("ppu"),
							struct(
									col("topping_id").alias("id"),
									col("topping_type").alias("type")
									).alias("topping"),
							col("type")
							)


					val ComplexDF1 = ComplexDF.groupBy("batter","id","name","ppu","type").agg(collect_list("topping").alias("topping"))
					//ComplexDF1.printSchema()

					val ComplexDF2 = ComplexDF1.select(
							struct(
									col("batter")
									).alias("batters"),
							col("id"),
							col("name"),
							col("ppu"),
							col("type"),
							col("topping")
							)
					ComplexDF2.printSchema()











	}
}