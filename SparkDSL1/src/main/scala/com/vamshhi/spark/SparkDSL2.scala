package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDSL2 {

	def main(args:Array[String]):Unit =
		{

				val spark = SparkSession.builder().appName("Spark DSL2").master("local[*]").getOrCreate()
						val txn_data = spark.read.format("csv").option("header","true").option("inferschema","true").load("file:///C:/Data/txns")
						txn_data.show()

						//selecting few columns : Operation 1
						val df1 = txn_data.select("txnno","txndate","amount","category","product","spendby")
						df1.show()

						//filtering based on conditions
						val df2 = txn_data.filter(col("category")==="Gymnastics" && col("spendby")==="cash")
						df2.show()

						//Operation 2
						val df3 = txn_data.filter(col("txnno")>50000 && col("spendby")==="cash")
						df3.show()

						//Operation 3
						val df4 = df3.filter(col("product") like("Weightlifting%"))
						df4.show()
		}
}