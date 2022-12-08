import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.HashMap

object DataMapping {
  // Triple Table schema
  case class Triple(sub: String, pred: String, obj: String)
  // 环境

  //sparkSession封装了SparkContext和SQLContext,并且在builder的getOrCreate方法中判断是否符合要求的SparkSession存在，有责是用，没有则进行创建
  val spark: SparkSession = Settings.sparkSession
  val _sc: SparkContext = spark.sparkContext
  import spark.implicits._


  def read(inputFile: String): DataFrame = {
    val tris = _sc.textFile(inputFile)
      .map(str => str.split(" "))
      .map(p => Triple(p(0), p(1), p(2)))
      .toDF()
    tris
  }
  def getPreds(tri: DataFrame): Seq[String] = {
    val seq = tri.select("pred").distinct()
      .collect().map(row => row.toString()).toSeq
    seq
  }
  def getSO(tri: DataFrame): Seq[String] = {

    val seq = tri.select("sub").distinct().collect().map(row => row.toString()).toSet
      .++(tri.select("obj").distinct().collect().map(row => row.toString()).toSet)
      .toSeq
    seq
  }
  def dataMapping(inputFile: String, outputDIR: String): Unit = {
    val fileName = inputFile.substring(inputFile.lastIndexOf("\\") + 1)
    val frame = read(inputFile)

    // PRED =========================
    val predArr = frame.select("pred").distinct()
      .map(row => row.toString())
      .rdd
      .zipWithIndex().collect()

    var preds = new HashMap[String, Long]
    predArr.foreach(tup => {
      preds += (tup._1.replace("[", "").replace("]", "").trim
        -> tup._2)
    })
    // TODO 存储 preds
    _sc.parallelize(preds.toSeq).toDF().write.parquet(outputDIR + File.separator + fileName + File.separator + fileName + ".p")

    // SO ============================
    val soArr = frame.select("sub").distinct().map(row => row.toString()).rdd
      .union(frame.select("obj").distinct().map(row => row.toString()).rdd).zipWithIndex().collect()
    var sos = new HashMap[String, Long]
    soArr.foreach(tup => {
      sos += (tup._1.replace("[", "").replace("]", "").trim
        -> tup._2)
    })
    // TODO 存储 so
    _sc.parallelize(sos.toSeq).toDF().write.parquet(outputDIR + File.separator + fileName + File.separator + fileName + ".so")

    // TRIPLES =====================================
    // TODO 存储 tri
    frame.map(row => {
      sos.get(row.get(0).toString).get + " " + preds.get(row.get(1).toString).get + " " + sos.get(row.get(2).toString).get
    }).toDF().write.parquet(outputDIR + File.separator + fileName + File.separator + fileName + ".tri")
    println("[triples] " + frame.count())
    println("[preds] " + preds.size)
    println("[so] " + sos.size)
    println("[数据mapping] done ")
  }

}
