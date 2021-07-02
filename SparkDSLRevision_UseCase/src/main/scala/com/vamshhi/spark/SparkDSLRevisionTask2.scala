package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object SparkDSLRevisionTask2 
{
	def main(args:Array[String]):Unit=
		{

				val spark = SparkSession.builder().appName("Spark DSL revision Task2").master("local[*]").getOrCreate()
						val devices_DF = spark.read.format("csv").option("delimiter","~").load("file:///C:/Data/devices.json")
						devices_DF.show(false)

						val devices_struct = StructType(
								StructField("device_id",StringType,false)::
									StructField("device_name",StringType,true)::
										StructField("humidity",StringType,true)::
											StructField("lat",StringType,true)::
												StructField("long",StringType,true)::
													StructField("scale",StringType,true)::
														StructField("temp",StringType,true)::
															StructField("timestamp",StringType,true)::
																StructField("zipcode",StringType,true)::Nil)
						val devices_DF1 = devices_DF.withColumn("_c0",from_json(col("_c0"),devices_struct))
						//devices_DF1.show(false)
						val devices_resDF = devices_DF1.select(col("_c0.*"))
						devices_resDF.show(false)
		}
}