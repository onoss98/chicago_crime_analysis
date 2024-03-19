package edu.ucr.cs.cs167.group24

import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SpatialAnalysis {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Project A - Task 2")

    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val inputFile: String = "Chicago_Crimes_ZIP"
    val outputFile: String = "ZIPCodeCrimeCount"
    import edu.ucr.cs.bdlab.beast._

    val df : DataFrame = sparkSession.read.parquet(inputFile)
    df.createOrReplaceTempView("crimes")

    //df.printSchema()
    val crime_count_df = sparkSession.sql("SELECT ZIPcode, count(*) as CrimeCount FROM crimes GROUP BY ZIPcode")
    crime_count_df.createOrReplaceTempView("crime_count_view")

    sparkSession.read.format("shapefile").load("tl_2018_us_zcta510.zip").createOrReplaceTempView("zips")

    sparkSession.sql("SELECT ZIPcode, geometry, CrimeCount FROM crime_count_view, zips WHERE ZIPcode = ZCTA5CE10").coalesce(1).write.mode(SaveMode.Overwrite).format("shapefile").save(outputFile)

    System.out.println("Created shapefile: ",outputFile)

    sparkSession.stop()
  }
}