package com.vamshhi.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf

object Spark_Avro_Task2 
{
	def main(args:Array[String]):Unit =
		{
				val Conf = new SparkConf().setAppName("Spark Avro Task2").setMaster("local[*]")
						val sc = new SparkContext(Conf)
						val spark = SparkSession.builder().getOrCreate()
						val Txn_records = sc.textFile("file:///C:/Data/Txns")
						val structsData = 
						StructType(
								StructField("txnno",StringType,true)::
									StructField("txndate",StringType,false)::
										StructField("custno",StringType,true)::
											StructField("amount",StringType,true)::
												StructField("category",StringType,false)::
													StructField("product",StringType,true)::
														StructField("city",StringType,true)::
															StructField("state",StringType,true)::
																StructField("spendby",StringType,false)::Nil)
						val Txns_row_rdd = Txn_records.map(x=>x.split(",")).map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
						val Txns_DF = spark.createDataFrame(Txns_row_rdd,structsData)
						Txns_DF.createOrReplaceTempView("Txns_tempView")
						val res = spark.sql("select * from Txns_tempView where category = 'Exercise & Fitness'")
						res.show()
						res.coalesce(1).write.format("com.databricks.spark.avro").mode("overwrite").save("file:///D:/D Data/ResultDir/Txns_avro")
		}
}