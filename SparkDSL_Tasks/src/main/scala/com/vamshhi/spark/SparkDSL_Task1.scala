package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDSL_Task1 
{

	def main(args:Array[String]):Unit =
		{

				val Conf = new SparkConf().setAppName("Spark DSL Task1").setMaster("local[*]")
						val Sc = new SparkContext(Conf)
						Sc.setLogLevel("Error")
						val spark = SparkSession.builder().getOrCreate()
						val df1 = spark.read.format("csv").option("header","true").option("inferschema","true").load("file:///C:/Data/f1.csv")
						val df2 = spark.read.format("csv").option("header","true").option("inferschema","true").load("file:///C:/Data/f2.csv")
						
						println("===============df1================")
						df1.show(false)
						
						println("===============df2================")
						df2.show(false)

						// inner join
						println("=========Inner Join================")
						val df_ij = df1.join(df2,Seq("txnno"),"inner")
						df_ij.show(false)

						//Left Outer join
						println("==============left Outer================")
						val df_loj = df1.join(df2,Seq("txnno"),"leftouter")
						df_loj.show(false)

						//Right Outer join
						println("===============right outer================")
						val df_roj = df1.join(df2,Seq("txnno"),"rightouter")
						df_roj.show(false)

						//Full Outer join
						println("==================full outer==============")
						val df_foj = df1.join(df2,Seq("txnno"),"fullouter")
						df_foj.show(false)
		}
}