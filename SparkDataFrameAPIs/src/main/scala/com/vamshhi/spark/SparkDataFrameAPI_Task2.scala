package com.vamshhi.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDataFrameAPI_Task2 {

	def main(args:Array[String]):Unit = {

			val spark = SparkSession.builder().appName("Spark dataframe API's Task 2").master("local[*]").getOrCreate()
					val usdata_DF = spark.read.format("csv").option("header","true").load("file:///C:/Data/usdata.csv")
					val get_domain = udf((value:String)=> value.split('.')(1))

					val df_final = usdata_DF
					.withColumn("web",get_domain(col("web")))
					df_final.show()
	}
}