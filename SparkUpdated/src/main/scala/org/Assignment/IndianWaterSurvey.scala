package org.Assignment

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, DataFrameNaFunctions, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, to_date, udf}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql


object IndianWaterSurvey {
  def main(args: Array[String]): Unit = {

    try {

      val conf = new SparkConf().setMaster("local[*]").setAppName("WaterSurvey")
      val sc = new SparkContext(conf)
      val spark = SparkSession.builder().master("local").appName("Assignment").enableHiveSupport().getOrCreate()

      val sqlContext = new SQLContext(sc)
      //import sqlContext.implicits._

      val waterQuality = spark.read.format("csv").option("header", true).load("file:///root/IndiaAffectedWaterQualityAreas.csv")




      val codesDistrict = spark.read.format("csv").option("header", true).load("file:///root/Districts Codes 2001.csv")


      val newDf = codesDistrict.select(regexp_replace(col("State Code"), " ", " -999").alias("State Code"), regexp_replace(col("District Code"), " ", " -999").alias("District Code")
        , col("Name of the State/Union territory and Districts"))




      val finalData = waterQuality.as("d1").join(newDf.as("d2"), col("d1.State Name") === col("d2.Name of the State/Union territory and Districts"), "inner")

      val qualityParameter = waterQuality.groupBy("Village Name","Year").agg(count("Quality Parameter"))

      qualityParameter.show()
      finalData.show()

      qualityParameter.write.format("orc").mode("append").saveAsTable("default.table6")


      finalData.write.format("orc").mode("append").saveAsTable("default.table5")


    }catch {
      case e: Exception => println("Exception Occured1 : " + e.printStackTrace())

    }

  }

}
