package com.vamshhi.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkPhase2Tasks {

	def main(args:Array[String]):Unit = {

			val Conf = new SparkConf().setAppName("Spark Phase 2 Task").setMaster("local[*]")
					val sc = new SparkContext(Conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()

					val txns_df = spark.read.format("csv").option("header","true").option("inferschema","true").load("file:///C:/Data/txns")
					txns_df.show(false)

					val txns_df1 = addIndexColumn(spark,txns_df)
					txns_df1.show(false)
	}

	def addIndexColumn(spark: SparkSession,df: DataFrame) = {
			spark.createDataFrame(
					df.rdd.zipWithIndex.map{
					case (row, index) => Row.fromSeq(row.toSeq :+ index)
					},
					//create schema for index column
					StructType(df.schema.fields :+ StructField("index",LongType,false)))
	}

	//	def addIndexColumn(df: DataFrame) = {
	//			val df1 = df.withColumn("index",monotonically_increasing_id())
	//					df1
	//	}
}