package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf

object SparkCovidUseCase 
{
	//case class
case class schema (Direction:String,Year:String,Date:String,Weekday:String,Current_Match:String,Country:String,Commodity:String,
		Transport_Mode:String,Measure:String,Value:String,Cumulative:String)

def main(args:Array[String]):Unit =
{

		val Conf = new SparkConf().setAppName("Spark Covid usecase").setMaster("local[*]")
				val sc = new SparkContext(Conf)
				sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
				val spark = SparkSession.builder().getOrCreate()
				import spark.implicits._

				//Step 1:
				val covid_data = sc.textFile("file:///C:/Data/effects-of-covid-19-on-trade-1-february-20-may-2020-provisional-csv.txt")
				val header = covid_data.first()
				val covid_data1 = covid_data.filter(x=>x!=header)
				val schema_covid_data = covid_data1.map(x=>x.split(",")).map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10)))
				val covid_tonnes = schema_covid_data.filter(x=>x.Measure.equals("Tonnes"))
				val tdf = covid_tonnes.toDF()
				tdf.show()

				//Step 2:
				val covid_row_rdd = covid_data1.map(x=>x.split(",")).map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10)))
				//reading schema file
				val schema1 = sc.textFile("file:///C:/Data/schema_file.txt").first()
				//dynamic Schema Creation using StructType
				val struct_schema = StructType(schema1.split(",").map(Column => StructField(Column,StringType,true)))
				val struct_DF = spark.createDataFrame(covid_row_rdd, struct_schema)
				struct_DF.createOrReplaceTempView("Covid_tab")
				val ddf = spark.sql("select * from Covid_tab where Measure='$'")
				ddf.show()

				//step 3:
				val tdf_temp_view = tdf.createOrReplaceTempView("tdf_temp_view")
				val ddf_temp_view = ddf.createOrReplaceTempView("ddf_temp_view")

				val texports = spark.sql("select * from tdf_temp_view where Direction = 'Exports'")
				//texports.show()
				//texports.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save("file:///D:/D Data/ResultDir/Covid_use_case_dir/texport_csv")


				val dexports = spark.sql("select * from ddf_temp_view where Direction = 'Exports'")
				//dexports.show()
				//dexports.coalesce(1).write.format("parquet").mode("overwrite").save("file:///D:/D Data/ResultDir/Covid_use_case_dir/dexport_parquet")

				val dimports = spark.sql("select * from ddf_temp_view where Direction = 'Imports'")
				//dimports.show()
				//dimports.coalesce(1).write.format("json").mode("overwrite").save("file:///D:/D Data/ResultDir/Covid_use_case_dir/dimport_json")

				val dreimports = spark.sql("select * from ddf_temp_view where Direction = 'Reimports'")
				//dreimports.show()
				//dreimports.coalesce(1).write.format("com.databricks.spark.avro").mode("overwrite").save("file:///D:/D Data/ResultDir/Covid_use_case_dir/dreimports_avro")

				//step 4:   

				val exports_csv = spark.read.format("csv").option("header","true").load("file:///D:/D Data/ResultDir/Covid_use_case_dir/texport_csv/")
				//exports_csv.show()
				val exports_parquet = spark.read.format("parquet").load("file:///D:/D Data/ResultDir/Covid_use_case_dir/dexport_parquet/")
				//exports_parquet.show()
				val imports_json = spark.read.format("json").load("file:///D:/D Data/ResultDir/Covid_use_case_dir/dimport_json/")
				//imports_json.show()
				val reimports_avro = spark.read.format("com.databricks.spark.avro").load("file:///D:/D Data/ResultDir/Covid_use_case_dir/dreimports_avro/")
				//reimports_avro.show()

				val covid_data_union = exports_csv.union(exports_parquet).union(imports_json).union(reimports_avro)

				//writing the above union DF as xml format 
				covid_data_union.coalesce(1).write.format("com.databricks.spark.xml").partitionBy("Direction")
				.option("rootTag","covid_data").option("rowTag","report").mode("overwrite")
				.save("file:///D:/D Data/ResultDir/Covid_use_case_dir/Covid_union_xml")		
}
}