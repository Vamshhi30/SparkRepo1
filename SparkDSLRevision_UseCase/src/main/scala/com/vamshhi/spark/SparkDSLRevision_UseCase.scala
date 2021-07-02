package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object SparkDSLRevision_UseCase 
{
case class schema(txnno:String,txndate:String,custno:String,amount:String,category:String,product:String,city:String,state:String,spendby:String)

def main(args:Array[String]):Unit =
{
		val Conf = new SparkConf().setAppName("Spark DSL Usecase").setMaster("local[*]")
				val sc = new SparkContext(Conf)
				val spark = SparkSession.builder().getOrCreate()

				import spark.implicits._

				//columns_schema 
				val Col_List = List("txnno","txndate","custno","amount","category","product","city","state","spendby")

				// 1.Create a scala List with 1,4,6,7 and Do an iteration and add 2 to it
				val intlist = List(1,4,6,7)
				val res_int = intlist.map(x=>x+2)
				res_int.foreach(println)

				println()

				// 2.Create a scala List with zeyobron,zeyo and analytics and filter elements contains zeyo
				val strlist = List("zeyobron","zeyo","analytics")
				val res_str = strlist.filter(x=>x.contains("zeyo"))
				res_str.foreach(println)

				println()

				// 3.Read file1 as an rdd and filter gymnastics rows
				val file1 = sc.textFile("file:///C:/Data/rev_data/file1.txt")
				val cat_filter = file1.filter(x=>x.contains("Gymnastics"))
				cat_filter.take(10).foreach(println)

				println()

				//4.Create a case class and Impose case class to it And filter product contains Gymnastics? 
				val schema_rdd = file1.map(x=>x.split(",")).map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
				val prod_filter = schema_rdd.filter(x=>x.product.contains("Gymnastics"))
				prod_filter.take(10).foreach(println)

				println()

				//5.Read file2 , convert it row rdd and  filter last index equals Cash-Completed
				val file2 = sc.textFile("file:///C:/Data/rev_data/file2.txt")
				val row_rdd = file2.map(x=>x.split(",")).map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
				val spendby_filter = row_rdd.filter(x=>x(8).toString().contains("cash"))
				spendby_filter.take(10).foreach(println)

				println()

				//6.Create dataframe using schema rdd and row rdd
				val caseclass_DF = prod_filter.toDF().select(Col_List.map(col):_*)
				caseclass_DF.show()

				println()

				val struct_schema = StructType(
						StructField("txnno",StringType,false)::
							StructField("txndate",StringType,false)::
								StructField("custno",StringType,false)::
									StructField("amount",StringType,false)::
										StructField("category",StringType,false)::
											StructField("product",StringType,false)::
												StructField("city",StringType,false)::
													StructField("state",StringType,false)::
														StructField("spendby",StringType,false)::Nil)

				val struct_DF = spark.createDataFrame(spendby_filter,struct_schema).select(Col_List.map(col):_*)
				struct_DF.show()

				println()

				//7.Read file 3 as csv with header true and inferschema and Show

				val csv_DF = spark.read.format("csv").option("header","true").option("inferschema","true").load("file:///C:/Data/rev_data/file3.txt").select(Col_List.map(col):_*)
				csv_DF.show()

				//8.Read file 4 as json and file 5 as parquet and show both the dataframe?
				val json_DF = spark.read.format("json").load("file:///C:/Data/rev_data/file4.json").select(Col_List.map(col):_*)
				json_DF.show()

				println()

				val parquet_DF = spark.read.format("parquet").load("file:///C:/Data/rev_data/file5.parquet").select(Col_List.map(col):_*)
				parquet_DF.show()

				println()

				//9.Read file 6 as xml with row tag as txndata
				val xml_DF = spark.read.format("com.databricks.spark.xml").option("rowTag","txndata").load("file:///C:/Data/rev_data/file6").select(Col_List.map(col):_*)
				xml_DF.show()

				//10.Define a unified column list and impose using select and Union all the dataframes
				//val Col_List = List("txnno","txndate","custno","amount","category","product","city","state","spendby") [Declared above]

				val union_DF = caseclass_DF.union(struct_DF).union(csv_DF).union(json_DF).union(parquet_DF).union(xml_DF)
				union_DF.show()

				println()

				//11.Get year from txn date and add one column at the end as status 1 for cash and 0 for credit in spendby and filter txnno<50000
				val res_DF = union_DF.withColumn("txndate",expr("split(txndate,'-')[2]"))
				val res_DF1 =  res_DF.withColumn("status",expr("case when spendby = 'cash' then 1 else 0 end")).filter(col("txnno")>50000)
				res_DF1.show()

				//12.Write as an avro in local with mode Append and partition the category column
				res_DF1.write.format("com.databricks.spark.avro").partitionBy("category").mode("append").save("file:///D:/D Data/ResultDir/Revision_Outputs/res_avro")
}
}