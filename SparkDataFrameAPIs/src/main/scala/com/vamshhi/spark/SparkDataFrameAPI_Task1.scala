package com.vamshhi.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDataFrameAPI_Task1 
{
	def main(args:Array[String]):Unit =
		{
				val spark = SparkSession.builder().appName("Spark DataFrame APIs").master("local[*]").getOrCreate()
						val usdata_DF1 = spark.read.format("csv").option("header","true").option("inferschema","true").load("file:///C:/Data/usdata.csv")
						val res = usdata_DF1.filter(col("state")==="LA")
						res.show()
		}
}