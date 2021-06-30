package com.vamshhi.spark

import org.apache.spark.sql.SparkSession

object Spark_Parquet_Task3 
{
	def main(args:Array[String]):Unit = 
		{
				val spark = SparkSession.builder().appName("Spark Parquet Task 3").master("local[*]").getOrCreate()
						val usdata_csv = spark.read.format("csv").option("header","true").load("file:///C:/Data/usdata.csv")
						usdata_csv.show()
						//writing usdata_csv as parquet with lzo compression
						spark.conf.set("spark.sql.parquet.compression.codec","gzip") 
						usdata_csv.write.format("parquet").mode("overwrite").save("file:///D:/D Data/ResultDir/usdata_parquet")
		}
}