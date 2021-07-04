package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source

object SparkDSL_Task3 {

	def main(args:Array[String]):Unit ={

			val Conf = new SparkConf().setAppName("Spark DSL Task3").setMaster("local[*]")
					val Sc = new SparkContext(Conf)
					Sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					val url_data = Source.fromURL("https://randomuser.me/api/0.8/?results=10").mkString
					//println(url_data)
					val url_Rdd = Sc.parallelize(List(url_data))
					val url_DF = spark.read.json(url_Rdd)
					url_DF.show(false)
					url_DF.printSchema()
	}
}