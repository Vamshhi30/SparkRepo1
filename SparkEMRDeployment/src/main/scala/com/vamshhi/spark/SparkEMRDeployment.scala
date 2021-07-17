package com.vamshhi.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkEMRDeployment {

	def main(args:Array[String]):Unit = {

			val spark = SparkSession.builder().master("local[*]").appName("Spark EMR Deployment").config("fs.s3a.access.key","AKIASV27A7AITJT6FBXQ").config("fs.s3a.secret.key","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").getOrCreate()
					val sc = spark.sparkContext
					sc.setLogLevel("Error")

					val txns_df = spark.read.format("csv").option("header","true").option("inferschema","true").load(args(0))

					txns_df.show(false)
					txns_df.printSchema()

					val Gym_df = txns_df.filter(col("category")==="Gymnastics")
					Gym_df.show(false)

					val cash_df = txns_df.filter(col("spendby")==="cash")
					cash_df.show(false)

					val union_df = Gym_df.union(cash_df)

					union_df.show(false)

					//union_df.coalesce(1).write.format("json").mode("overwrite").save(args(1))
					
					union_df.coalesce(1).write.format("com.databricks.spark.avro").mode("append").save(args(1))
					
					print("===========Data Written==============")
	}
}