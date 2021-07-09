package com.vamshhi.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.io.Source

object SparkComplexDataProcessing 
{
	def main(args:Array[String]):Unit = 
		{
				val Conf = new SparkConf().setAppName("Spark Complex data processing").setMaster("local[*]")
						val sc = new SparkContext(Conf)
						val spark = SparkSession.builder().getOrCreate()
						sc.setLogLevel("Error")

						//avro dataset read
						val avro_df = spark.read.format("com.databricks.spark.avro").load("file:///C:/Data/Spark Project (Phase 1) Dataset/part-00000-1bd5ec9a-4ceb-448c-865f-305f43a0b8a9-c000.avro")
						println("=====================================Raw Avro DF======================================")
						avro_df.show(false)

						//randomuser API Complex Json read
						val randomAPI_url = Source.fromURL("https://randomuser.me/api/0.8/?results=200").mkString
						val randomAPI_rdd = sc.parallelize(List(randomAPI_url))
						val randomAPI_df = spark.read.json(randomAPI_rdd)
						println("=====================================randomAPI_df Raw Complex Json======================================")
						randomAPI_df.show(false)
						randomAPI_df.printSchema()

						//flattening complex Json 
						val flattenDF = randomAPI_df.withColumn("results",explode(col("results"))).select(
								col("nationality"),
								col("results.user.cell"),
								col("results.user.dob"),
								col("results.user.email"),
								col("results.user.location.*"),
								col("results.user.md5"),
								col("results.user.name.*"),
								col("results.user.password"),
								col("results.user.phone"),
								col("results.user.picture.*"),
								col("results.user.registered"),
								col("results.user.salt"),
								col("results.user.sha1"),
								col("results.user.sha256"),
								col("results.user.username"),
								col("seed"),
								col("version")
								)
						println("=====================================flattenDF Json DF=========================================")
						flattenDF.show(false)
						flattenDF.printSchema()

						val flattenDF1 = flattenDF.withColumn("username",regexp_replace(col("username"),"\\d",""))
						println("=====================================Final flattenDF Json DF=========================================")
						flattenDF1.show(false)

						//applying broadcast left outer join on dataframes [flattenDF1 and avro_df]

						val df_join = avro_df.join(broadcast(flattenDF1),Seq("username"),"left")

						println("=====================================Joined DF=========================================")
						df_join.show(false)
						//println(avro_df.count())
						//println(flattenDF1.count())
						//println(df_join.count())
						df_join.printSchema()

						df_join.persist()

						val available_cust = df_join.filter(col("nationality").isNotNull)

						val non_available_cust = df_join.filter(col("nationality").isNull)

						println("=====================================available_cust DF=========================================")
						val available_custDF = available_cust.withColumn("today",current_date())
						available_custDF.show(false)

						println("=====================================non_available_cust DF=========================================")
						val non_available_cust1 = non_available_cust.na.fill("NA").na.fill(0)
						val non_available_custDF = non_available_cust1.withColumn("today",current_date())
						non_available_custDF.show(false)
						df_join.unpersist()

						val available_cust_json = available_custDF.groupBy("username").agg(
								collect_list("ip").alias("ip"),
								collect_list("id").alias("id"),
								sum("amount").alias("total_amount"),
								struct(
										count("ip").alias("ip_count"),
										count("id").alias("id_count")
										).alias("count")
								)
						available_cust_json.show(false)
						available_cust_json.printSchema()
						available_cust_json.coalesce(1).write.format("json").mode("overwrite").save("file:///D:/D Data/ResultDir/Spark Project1_Phase_dir/available_cust")
						println("==============Data Written=============")
						
						val non_available_cust_json = non_available_custDF.groupBy("username").agg(
								collect_list("ip").alias("ip"),
								collect_list("id").alias("id"),
								sum("amount").alias("total_amount"),
								struct(
										count("ip").alias("ip_count"),
										count("id").alias("id_count")
										).alias("count")
								)

						non_available_cust_json.show(false)
						non_available_cust_json.printSchema()
						non_available_cust_json.coalesce(1).write.format("json").mode("overwrite").save("file:///D:/D Data/ResultDir/Spark Project1_Phase_dir/non_available_cust")
						println("==============Data Written=============")
		}  
}
