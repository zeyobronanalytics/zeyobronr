package SparkPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.io.Source
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive._



object SparkObj {

case class schema(txnno:String, txndate:String, custno:String, amount:String,category:String, product:String, city:String, state:String, spendby:String)

def main(args:Array[String]):Unit={

		val conf=new SparkConf().setAppName("spark_integration").setMaster("local[*]")
				val sc=new SparkContext(conf)
				sc.setLogLevel("Error")
				val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
				import spark.implicits._


				println("=====================1=======================")
				val lisint= List(1,4,6,7).map(x=>x*2).foreach(println)
				
				
				val columns=List("txnno","txndate","custno","amount","category","product","city","state","spendby")

				println
				println("==============================2==============")

				val lisstr= List("zeyobron","zeyo","analytics").filter(x=>x.contains("zeyo")).foreach(println)


				println
				println("=====================3=======================")




				val file1= sc.textFile(args(0))


				file1.filter(x=>x.contains("Gymnastics")).take(10)

				val mapsplit = file1.map(x=>x.split(","))
				val schemardd= mapsplit.map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

				println
				println("=====================4=======================")
				println

				schemardd.filter(x=>x.product.contains("Gymnastics")).take(10).foreach(println)



				println
				println("=====================5=======================")
				println

				val rowrdd = sc.textFile(args(1)).map(x=>x.split(",")).map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
				.filter(x=>x(8).toString().contains("cash"))


				rowrdd.take(10).foreach(println)


				println
				println("=====================6=======================")
				println



				val schemadf = schemardd.toDF().select(columns.map(col):_*)
				schemadf.show(5)



				val Structschema = StructType(Array(
						StructField("txnno",StringType,true),
						StructField("txndate",StringType,true),
						StructField("custno",StringType,true),
						StructField("amount", StringType, true),
						StructField("category", StringType, true),
						StructField("product", StringType, true),
						StructField("city", StringType, true),
						StructField("state", StringType, true),
						StructField("spendby", StringType, true)
						))



				val rowdf= spark.createDataFrame(rowrdd, Structschema).select(columns.map(col):_*)





				println
				println("=====================7======================")
				println



				val csvdf = spark.read.format("csv").option("header", "true").load(args(2)).select(columns.map(col):_*)

				csvdf.show()




				println
				println("=====================8=======================")
				println


				val jsondf = spark.read.format("json").load(args(3)).select(columns.map(col):_*)

				jsondf.show()

				val parquetdf = spark.read.format("parquet").load(args(4)).select(columns.map(col):_*)

				parquetdf.show()


				println
				println("=====================9=======================")
				println


				val xmldf = spark.read.format("com.databricks.spark.xml").option("rowTag","txndata").load(args(5)).select(columns.map(col):_*)

				xmldf.show()




				println
				println("=====================10=======================")
				println



				val uniondf = schemadf.union(rowdf).union(csvdf).union(jsondf).union(parquetdf).union(xmldf).select(columns.map(col):_*)


				uniondf.show()

				
				println
				println("=====================11=======================")
				println

				val finaldf=		uniondf.withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))
				
				
				finaldf.show()
				
				println
				println("=====================12=======================")
				println
				
				
				finaldf.write.format("com.databricks.spark.avro").partitionBy("category").mode("append").save(args(6))
				

}
}