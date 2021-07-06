package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkComplexJson7 {

	def main(args:Array[String]):Unit ={

			val Conf = new SparkConf().setAppName("Spark Complex Json").setMaster("local[*]")
					val sc = new SparkContext(Conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					val usersDF = spark.read.format("json").option("multiLine","true").load("file:///C:/Data/complexjson/users.json")

					println("========================Raw Complex Json==========================")
					usersDF.show(false)
					usersDF.printSchema()
					println("========================Flattened Json==========================")
					val flattenDF = usersDF.withColumn("users",explode(col("users"))).select(
							col("users.emailAddress"),
							col("users.firstName"),
							col("users.lastName"),
							col("users.phoneNumber"),
							col("users.userId")
							)
					flattenDF.show(false)
					flattenDF.printSchema()

					println("========================Reverting Back to Complex Json==========================")

					val ComplexDF = flattenDF.select(
							struct(
									col("emailAddress"),
									col("firstName"),
									col("lastName"),
									col("phoneNumber"),
									col("userId")
									).alias("users")
							).groupBy().agg(collect_list("users").alias("users"))

					ComplexDF.show(false)
					ComplexDF.printSchema()
	}

}