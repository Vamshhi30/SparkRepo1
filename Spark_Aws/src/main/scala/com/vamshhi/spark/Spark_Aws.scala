package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Spark_Aws {

	def main(args:Array[String]):Unit = {

			val Conf = new SparkConf().setAppName("Spark AWS").setMaster("local[*]")
					val sc = new SparkContext(Conf)
					val spark = SparkSession.builder().config("fs.s3a.access.key","").config("fs.s3a.secret.key","").getOrCreate()
					sc.setLogLevel("Error")
					val df = spark.read.format("parquet").load("s3a://zeyonifibucket/cashdata_parquet/part-00000-5e2a219f-21ea-454c-9c18-8859c6df617f-c000.snappy.parquet")
					df.show(false)
					df.printSchema()

					val res_df = df.groupBy("category","spendby").agg(sum("amount").alias("total_amount"))
					res_df.show(false)

					res_df.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save("file:///D:/D Data/ResultDir/Vamshhi_Dir")
					println("====================Data Written to S3=============================")

					//String interpolation
					val Name = "Vamshhi"
					val Occupation = "Software Engineer"
					val Company = "Tata Consultancy Services"
					val phoneNum = "8977342394"

					println(s"Hi! This is $Name,Currently working as $Occupation at $Company. My Contact Number is $phoneNum")

	}
}
