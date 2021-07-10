package com.vamshhi.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkPhase2Task2 
{
	def main(args:Array[String]):Unit = 
		{
				val yest_date = java.time.LocalDate.now.minusDays(1).toString()
						println(s"=============Yesterday's Date is $yest_date============================")
		}
}