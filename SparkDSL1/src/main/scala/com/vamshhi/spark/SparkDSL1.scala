package com.vamshhi.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDSL1 {

	def main(args:Array[String]):Unit =
		{
				val spark = SparkSession.builder().appName("Spark DSL 1").master("local[*]").getOrCreate()
						val Txns_data = spark.read.format("csv").option("header","true").option("inferschema","true").load("file:///C:/Data/txns")
						Txns_data.show()

						//selecting txnno,category,product
						val Txns_select = Txns_data.select("txnno","category","product")
						Txns_select.show()

						//filtering category = "Gymnastics"
						val Txns_filter = Txns_data.filter(col("category")==="Gymnastics")
						Txns_filter.show()

						//writing filtered data as parquet
						//Txns_filter.coalesce(1).write.partitionBy("spendby").format("parquet").mode("overwrite").save("file:///D:/D Data/ResultDir/Txns_Parquet")

						//writing filtered data as avro
						//Txns_filter.coalesce(1).write.format("com.databricks.spark.avro").mode("overwrite").save("file:///D:/D Data/ResultDir/Txns_avro")

						//writing filtered data as xml
						Txns_filter.coalesce(1).write.format("com.databricks.spark.xml").option("rootTag","Transactions").option("rowTag","Transaction").mode("overwrite").save("file:///D:/D Data/ResultDir/Txns_xml")		
		}
}