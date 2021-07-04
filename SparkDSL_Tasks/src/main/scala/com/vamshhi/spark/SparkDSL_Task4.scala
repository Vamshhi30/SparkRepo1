package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkDSL_Task4 
{
	def main(args:Array[String]):Unit =
		{
				val Conf = new SparkConf().setAppName("Spark DSL Task4").setMaster("local[*]")
						val Sc = new SparkContext(Conf)
						Sc.setLogLevel("Error")
						val spark = SparkSession.builder().getOrCreate()
						val DF1 = spark.read.format("csv").option("delimiter","~").load("file:///C:/Data/devices.json")
						DF1.show(false)
						val Json_Struct = StructType(
								StructField("device_id",StringType,false)::
									StructField("device_name",StringType,true)::
										StructField("humidity",StringType,true)::
											StructField("lat",StringType,true)::
												StructField("long",StringType,true)::
													StructField("scale",StringType,true)::
														StructField("temp",StringType,true)::
															StructField("timestamp",StringType,true)::
																StructField("zipcode",StringType,true)::Nil)
						val DF2 = DF1.withColumn("_c0",from_json(col("_c0"), Json_Struct)).select(col("_c0.*"))
						println("=======================from_json==========================")
						DF2.show(false)
						val DF3 = DF2.select(to_json(struct(col("*"))).alias("_c0"))
						println("=======================to_json============================")
						DF3.show(false)
		}
}