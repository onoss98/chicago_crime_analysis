package edu.ucr.cs.cs167.onoss001

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.collection.Map

import edu.ucr.cs.bdlab.beast._


object PreprocessCSV {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    //initializing spark sessions and arguments
    val spark = SparkSession
      .builder()
      .appName("PreprocessCSV")
      .config(conf)
      .getOrCreate()
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession = spark)
    val sparkContext = spark.sparkContext
    val inputfile: String = args(0)


    import scala.collection.Map
    try {
      import spark.implicits._
      import edu.ucr.cs.bdlab.beast._

        val df = spark.read.format("csv")
          .option("sep", ",")
          .option("inferSchema", "true")
          .option("header", "true")
          .load(inputfile)
          .withColumnRenamed("Case Number", "CaseNumber")
          .withColumnRenamed("Primary Type", "PrimaryType")
          .withColumnRenamed("Location Description", "LocationDescription")
          .withColumnRenamed("Community Area", "CommunityArea")
          .withColumnRenamed("FBI Code", "FBICode")
          .withColumnRenamed("X Coordinate", "XCoordinate")
          .withColumnRenamed("Y Coordinate", "YCoordinate")
          .withColumnRenamed("FBI Code", "FBICode")
          .withColumnRenamed("Updated On", "UpdatedOn")

        df.printSchema()
        df.show()

      //---introduce geometry column---
      val crimesRDD: SpatialRDD = df.selectExpr("*", "ST_CreatePoint(x, y) AS geometry").toSpatialRDD
      println("Geometry created")
      val zipsRDD: SpatialRDD = sparkContext.shapefile("/Users/omar/Downloads/tl_2018_us_zcta510.zip")
      println("Zips Loaded")
      val crimeZipRDD: RDD[(IFeature, IFeature)] = crimesRDD.spatialJoin(zipsRDD)
      println("RDDs Joined")
      val crimeZip: DataFrame = crimeZipRDD.map({ case (geometry, zipcode) => Feature.append(geometry, zipcode.getAs[String]("ZCTA5CE10"), "ZIPCode") })
        .toDataFrame(spark)
        .drop("geometry")
      println("Dataframe Created")

      crimeZip.printSchema()
      crimeZip.write.mode(SaveMode.Overwrite).parquet("Chicago_Crimes_ZIP")

    } finally {
      spark.stop
    }
  }
}
