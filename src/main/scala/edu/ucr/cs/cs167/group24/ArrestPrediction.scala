package edu.ucr.cs.cs167.group24

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, StringIndexer, Tokenizer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


object ArrestPrediction {

  def main(args : Array[String]) {
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167 Task 4")
      .config(conf)
      .getOrCreate()
    val t1 = System.nanoTime
    try {
      val rawData: DataFrame = spark.read.parquet("Chicago_Crimes_ZIP.parquet")
      val arrestsDF: DataFrame = rawData.filter(rawData("Arrest").isin("true", "false"))
      val tokenizer = new Tokenizer().setInputCol("PrimaryType").setInputCol("Description").setOutputCol("words")
      val hashingTF = new HashingTF().setInputCol("words").setOutputCol("features")
      val stringIndexer = new StringIndexer().setInputCol("Arrest").setOutputCol("label").setHandleInvalid("skip")
      val svc = new LinearSVC()
//      arrestsDF.printSchema()
//      arrestsDF.show()
      val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, stringIndexer, svc))
      val paramGrid: Array[ParamMap] = new ParamGridBuilder()
        .addGrid(hashingTF.numFeatures, Array(1024, 2048))
        .addGrid(svc.fitIntercept, Array(true,false))
        .addGrid(svc.regParam, Array(0.01, 0.0001))
        .addGrid(svc.maxIter, Array(10, 15))
        .addGrid(svc.threshold, Array(0, 0.25))
        .addGrid(svc.tol, Array(0.0001, 0.01))
        .build()
      val cv = new TrainValidationSplit()
        .setEstimator(pipeline)
        .setEvaluator(new BinaryClassificationEvaluator())
        .setEstimatorParamMaps(paramGrid)
        .setTrainRatio(0.8)
        .setParallelism(2)
      val Array(trainingData: Dataset[Row], testData: Dataset[Row]) = arrestsDF.randomSplit(Array(0.8, 0.2))
      val model: TrainValidationSplitModel = cv.fit(trainingData)
      val numFeatures: Int = model.bestModel.asInstanceOf[PipelineModel].stages(1).asInstanceOf[HashingTF].getNumFeatures
      val fitIntercept: Boolean = model.bestModel.asInstanceOf[PipelineModel].stages(3).asInstanceOf[LinearSVCModel].getFitIntercept
      val regParam: Double = model.bestModel.asInstanceOf[PipelineModel].stages(3).asInstanceOf[LinearSVCModel].getRegParam
      val maxIter: Double = model.bestModel.asInstanceOf[PipelineModel].stages(3).asInstanceOf[LinearSVCModel].getMaxIter
      val threshold: Double = model.bestModel.asInstanceOf[PipelineModel].stages(3).asInstanceOf[LinearSVCModel].getThreshold
      val tol: Double = model.bestModel.asInstanceOf[PipelineModel].stages(3).asInstanceOf[LinearSVCModel].getTol
      val predictions: DataFrame = model.transform(testData)
      predictions.select("PrimaryType", "Description", "Arrest", "label", "prediction").show()
//      predictions.printSchema()
      val binaryClassificationEvaluator = new BinaryClassificationEvaluator()
        .setLabelCol("label")
        .setRawPredictionCol("prediction")
      val accuracy: Double = binaryClassificationEvaluator.evaluate(predictions)
      println(s"Accuracy of the test set is $accuracy")
      println(s"numFeatures: $numFeatures\nfitIntercept: $fitIntercept\nregParam: $regParam\nmaxIter: $maxIter\n" +
        s"threshold: $threshold\ntol: $tol")
      val t2 = System.nanoTime
      println(s"Applied sentiment analysis algorithm on input in ${(t2 - t1) * 1E-9} seconds")
    } finally {
      spark.stop
    }
  }
}
