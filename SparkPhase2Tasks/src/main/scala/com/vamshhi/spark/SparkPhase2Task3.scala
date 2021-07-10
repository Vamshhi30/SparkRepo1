package com.vamshhi.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkPhase2Task3 
{
	def main(args:Array[String]):Unit = 
		{
				val spark = SparkSession.builder().appName("Spark phase 2 tasks").master("local[*]").getOrCreate()
						val df1 = spark.read.format("csv").option("header","true").option("inferschema","true").load("file:///C:/Data/file1.txt")
						val df2 = spark.read.format("csv").option("header","true").option("inferschema","true").load("file:///C:/Data/file2.txt")
						//performing left anti join on df1 and df2
						val res = df1.join(df2,Seq("id"),"leftanti")
						res.show(false)
		}
}