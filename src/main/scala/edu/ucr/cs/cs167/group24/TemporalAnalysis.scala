package edu.ucr.cs.cs167.group24

import edu.ucr.cs.bdlab.beast.JavaSpatialSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.beast.{CRSServer, SparkSQLRegistration}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object TemporalAnalysis {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("Arguments must be 2! (i.e. start date, end date)")
      System.exit(1)
    }

    val inputFile: String = args(0)
    val outputFile: String = "CrimeTypeCount"

    val conf = new SparkConf().setAppName("CS167 Project: Task 3")

    if (!conf.contains("spark.master")) conf.setMaster("local[*]")

    val sparkSession = SparkSession.builder.config(conf).getOrCreate
    val sparkContext = new JavaSpatialSparkContext(sparkSession.sparkContext)

    // store information in sparkContext
    CRSServer.startServer(sparkContext)
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    try {
      //val parquetFile = args(0)   // initialize parquet file

      // define start and end dates; change as arguments if necessary
      val startDate = args(1)
      val endDate = args(2)

      val resultDF = sparkSession.read.parquet(inputFile)
        .withColumn("CrimeDate", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))
        // START OF: filter crimes between start and end dates
        .where(col("CrimeDate").between(
          to_timestamp(lit(startDate), "MM/dd/yyyy"),
          to_timestamp(lit(endDate), "MM/dd/yyyy")
        ))
        // END OF: filter crimes between start and end dates
        .groupBy("PrimaryType")
        .agg(count("*").alias("count"))

      // test results in terminal
      //resultDF.show()

      // output file
      resultDF.coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(outputFile)
    } finally {
      sparkSession.stop
    }
  }
}
