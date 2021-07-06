package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkComplexJson9 {

	def main(args:Array[String]):Unit = {

			val Conf = new SparkConf().setAppName("Spark Complex Json").setMaster("local[*]")
					val sc = new SparkContext(Conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					val battersDF = spark.read.format("json").option("multiLine","true").load("file:///C:/Data/complexjson/batters.json")

					println("===================Raw Complex Json====================")
					battersDF.show(false)
					battersDF.printSchema()

					val flattenDF = battersDF.withColumn("results",explode(col("results")))
					.select(
							col("results.batters.batter"),
							col("results.id"),
							col("results.name"),
							col("results.ppu"),
							col("results.topping"),
							col("results.type")
							).withColumn("batter",explode(col("batter")))
					.withColumn("topping",explode(col("topping")))

					val flattenDF1 = flattenDF.select(
							col("batter.id").alias("batter_id"),
							col("batter.type").alias("batter_type"),
							col("id"),
							col("name"),
							col("ppu"),
							col("topping.id").alias("topping_id"),
							col("topping.type").alias("topping_type"),
							col("type")
							)
					println("===================Flattened Json====================")
					flattenDF1.show(false)
					flattenDF1.printSchema()
					println("===================Reverting Back to Complex Json====================")

					val ComplexDF = flattenDF1.select(
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
							).groupBy("id","name","ppu","topping","type").agg(struct(collect_list("batter").alias("batter")).alias("batters"))

					val ComplexDF1 = ComplexDF.groupBy().agg(collect_list(struct("batters","id","name","ppu","topping","type")).alias("results"))

					ComplexDF1.show(false)
					ComplexDF1.printSchema()
	}
}