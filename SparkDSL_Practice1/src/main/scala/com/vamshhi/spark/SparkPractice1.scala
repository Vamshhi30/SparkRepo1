package com.vamshhi.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkPractice1 
{
	def main(args:Array[String]):Unit =
		{
				val spark = SparkSession.builder().appName("Spark DSL Practice").master("local[*]").getOrCreate()
						val txns_DF = spark.read.format("csv").option("header","true").option("inferschema","true").load("file:///C:/Data/txns")
						txns_DF.show()

						//(1)(i) DF.select() DSL API
						val txns_DF1 = txns_DF.select("category","product","spendby")
						txns_DF1.show()

						//(1)(ii) DF.select() DSL API (passing list values to select api)
						val columns = List("category","product","spendby")
						val txns_DF2 = txns_DF.select(columns.map(col):_*)
						txns_DF2.show()

						//(2)(i) DF.filter() DSL API - AND Operation
						val txns_DF3 = txns_DF.filter(col("category")==="Gymnastics" && col("spendby")==="cash")
						txns_DF3.show()

						//(2)(ii) DF.filter() DSL API - not equals Operation
						val txns_DF4 = txns_DF.filter(col("spendby")=!="cash")
						txns_DF4.show()

						//(2)(iii) DF.filter() DSL API - not equals Operation
						val txns_DF5 = txns_DF.filter(col("category")==="Gymnastics" || col("spendby")==="cash")
						txns_DF5.show()

						//(2)(iv) DF.filter() DSL API - is in Operation
						val txns_DF6 = txns_DF.filter(col("category").isin("Gymnastics","Team Sports","Exercise & Fitness"))
						txns_DF6.show()

						//(2)(v) DF.filter() DSL API - like operation
						val txns_DF7 = txns_DF.filter(col("product").like("Gymnastics%"))
						txns_DF7.show()

						//Operation 1:
						val Op1 = txns_DF.select("txnno","txndate","amount","category","product","spendby")
						Op1.show()

						//Operation 2:
						val Op2 = txns_DF.filter(col("txnno")>50000 && col("spendby")==="cash").filter(col("product").like("Weightlifting%"))
						Op2.show()

						// Expressions:
						//(3)(i) DF.selectExpr()
						val txns_DF8 = txns_DF.selectExpr("txnno","split(txndate,'-')[2] as year","amount")
						txns_DF8.show()

						//(3)(ii) DF.withColumn() [Existing column]
						val txns_DF9 = txns_DF.withColumn("txndate",expr("split(txndate,'-')[2]"))
						txns_DF9.show()

						//[non-Existing column]
						val txns_DF10 = txns_DF.withColumn("year",expr("split(txndate,'-')[2]"))
						txns_DF10.show()

						//(4). DF.withColumnRenamed("Old_col_name","New_col_name")
						val txns_DF11 = txns_DF9.withColumnRenamed("txndate","date")
						txns_DF11.show()

						//Operation 3:
						val Op3 = txns_DF.withColumn("category",expr("split(category,' ')[0]"))
						Op3.show()

						//Operation 4:
						val Op4 = txns_DF.withColumn("cat1",expr("split(category,' ')[0]"))
						Op4.show()

						//(3)(iii) DF.withColumn() [Passing Hard coded Values - String literal]
						val txns_DF12 = txns_DF.withColumn("check",lit("1"))
						txns_DF12.show()

						//[Passing Hard coded Values - Integer literal]
						val txns_DF13 = txns_DF.withColumn("check",lit(1))
						txns_DF13.show()

						//Conditional Expressions:
						//single condition:
						val txns_DF14 = txns_DF.withColumn("check",expr("case when spendby = 'credit' then 1 else 0 end"))
						txns_DF14.show()

						//multiple condition:
						val txns_DF15 = txns_DF.withColumn("check",expr("case when spendby = 'credit' then 1 when spendby = 'cash' then 0 else 'NA' end"))
						txns_DF15.show()
		}
}