package com.vamshhi.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDataFrameAPI_Task3 {

	def main(args:Array[String]):Unit ={
			val spark = SparkSession.builder().appName("Spark Dataframe APIs Task3").master("local[*]").getOrCreate()
					val txns_DF = spark.read.format("csv").option("header","true").option("inferschema","true").load(args(0))
					//txns_DF.show()
					val txns_filter = txns_DF.filter(!col("category").isin("Gymnastics","Team Sports"))
					//txns_filter.show()
					val txns_res = txns_filter.withColumn("cat1",expr("case when category = 'Exercise & Fitness' or category = 'Outdoor Recreation' then 'Outdoor' else 'Indoor' end"))
					//txns_res.show()
					txns_res.coalesce(1).write.format("com.databricks.spark.avro").mode("overwrite").save(args(1))             
	}
}