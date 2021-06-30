package com.vamshhi.spark
import org.apache.spark.sql.SparkSession

object SparkXmlTask4 
{

	def main(args:Array[String]):Unit = 
		{
				val spark = SparkSession.builder().appName("Spark Xml Task 4").master("local[*]").getOrCreate()
						val devices_json = spark.read.format("json").load(args(0))
						devices_json.write.format("com.databricks.spark.xml").option("rootTag","Devices")
						.option("rowTag","Fields").save(args(1))
		}
}