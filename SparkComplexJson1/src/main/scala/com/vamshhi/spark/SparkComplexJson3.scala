package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkComplexJson3 {

	def main(args:Array[String]):Unit ={

			val Conf = new SparkConf().setAppName("Spark Complex Json 3").setMaster("local[*]")
					val Sc = new SparkContext(Conf)
					val spark = SparkSession.builder().getOrCreate()
					Sc.setLogLevel("Error")
					val battersDF = spark.read.format("json").option("multiLine","true").load("file:///C:/Data/complexjson/batters.json").withColumn("results",explode(col("results")))
					println("===============Raw Nested Json Data==================")
					battersDF.show(false)
					battersDF.printSchema()

					println("===============Flattened Json Data==================")
					val FlattenDF = battersDF.select(
							col("results.batters.batter"),
							col("results.id"),
							col("results.name"),
							col("results.ppu"),
							col("results.topping"),
							col("results.type")
							).withColumn("batter",explode(col("batter"))).withColumn("topping",explode(col("topping")))

					val FlattenDF1 = FlattenDF.select(
							col("batter.id").alias("b_id"),
							col("batter.type").alias("b_type"),
							col("id"),
							col("name"),
							col("ppu"),
							col("topping.id").alias("t_id"),
							col("topping.type").alias("t_type"),
							col("type")
							)

					FlattenDF1.show(false)
					FlattenDF1.printSchema()

					println("===============Reverting Back to Complex Nested Json==================")

					val complexDF = FlattenDF1.select(
							struct(
									col("b_id").alias("id"),
									col("b_type").alias("type")
									).alias("batter"),
							col("id"),
							col("name"),
							col("ppu"),
							struct(
									col("t_id").alias("id"),
									col("t_type").alias("type")
									).alias("topping"),
							col("type")
							).groupBy("id","name","ppu","topping","type").agg(collect_list("batter").alias("batter"))
					.groupBy("id","name","ppu","batter","type").agg(collect_list("topping").alias("topping"))

					val complexDF1 = complexDF.select(
							struct(
									col("batter")
									).alias("batters"),
							col("id"),
							col("name"),
							col("ppu"),
							col("topping"),
							col("type")
							)
					val complexDF2 = complexDF1.select(
							struct(
									col("batters"),
									col("id"),
									col("name"),
									col("ppu"),
									col("topping"),
									col("type")
									).alias("results")    
							)
					complexDF2.show(false)
					complexDF2.printSchema()
	}

}